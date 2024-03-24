package org.baylight.redis.streams;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.TestConstants;
import org.junit.jupiter.api.Test;

public class StreamsWaitManagerTest implements WithAssertions, TestConstants {

    @Test
    void testRead_emptyResultAfterWait() {

        Map<String, RedisStreamData> streams = Map.of(
                "s1", mock(RedisStreamData.class));
        Map<String, StreamId> startIds = Map.of(
                "s1", StreamId.of(0, 1));
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(20L).thenReturn(120L);
        Map<String, List<StreamValue>> result = StreamsWaitManager.INSTANCE.readWithWait(streams,
                startIds, 0, clock, 100);

        assertThat(result).isEqualTo(Map.of("s1", List.of()));

        verify(clock, times(2)).millis();
        verify(streams.get("s1")).readNextValues(RedisStreamData.MAX_READ_COUNT,
                startIds.get("s1"));
        verifyNoMoreInteractions(clock, streams.get("s1"));
    }

    @Test
    void testRead_lessThanCountResultAfterWait() {

        Map<String, RedisStreamData> streams = Map.of(
                "s1", mock(RedisStreamData.class),
                "s2", mock(RedisStreamData.class));
        Map<String, StreamId> startIds = Map.of(
                "s1", StreamId.of(0, 1),
                "s2", StreamId.of(9, 9));
        when(streams.get("s1").readNextValues(anyInt(), any()))
                .thenReturn(List.of(mock(StreamValue.class)));
        when(streams.get("s2").readNextValues(anyInt(), any()))
                .thenReturn(List.of(mock(StreamValue.class)));
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(20L).thenReturn(120L);
        Map<String, List<StreamValue>> result = StreamsWaitManager.INSTANCE.readWithWait(streams,
                startIds, 4, clock, 100);

        verify(clock, times(2)).millis();
        assertThat(result.keySet()).isEqualTo(Set.of("s1", "s2"));
        assertThat(result.values().stream().collect(Collectors.summingInt(List::size)))
                .isEqualTo(2);

        verify(streams.get("s1")).readNextValues(4, startIds.get("s1"));
        verify(streams.get("s2")).readNextValues(4, startIds.get("s2"));
        verifyNoMoreInteractions(clock, streams.get("s1"), streams.get("s2"));
    }

    @Test
    void testRead_noWaitIfResultAfterWait() {

        Map<String, RedisStreamData> streams = Map.of(
                "s1", mock(RedisStreamData.class),
                "s2", mock(RedisStreamData.class));
        Map<String, StreamId> startIds = Map.of(
                "s1", StreamId.of(0, 1),
                "s2", StreamId.of(9, 9));
        when(streams.get("s1").readNextValues(anyInt(), any()))
                .thenReturn(List.of(mock(StreamValue.class), mock(StreamValue.class)));
        when(streams.get("s2").readNextValues(anyInt(), any()))
                .thenReturn(List.of(mock(StreamValue.class)));
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(20L).thenReturn(120L);

        Map<String, List<StreamValue>> result = StreamsWaitManager.INSTANCE.readWithWait(streams,
                startIds, 2, clock, 100);
        assertThat(result.keySet()).isEqualTo(Set.of("s1", "s2"));
        assertThat(result.values().stream().collect(Collectors.summingInt(List::size)))
                .isEqualTo(3);

        verify(clock, times(1)).millis();
        verify(streams.get("s1")).readNextValues(2, startIds.get("s1"));
        verify(streams.get("s2")).readNextValues(2, startIds.get("s2"));
        verifyNoMoreInteractions(clock, streams.get("s1"), streams.get("s2"));
    }

    @Test
    void testRead_notifyWakesUpDuringWait() throws Exception {

        Map<String, RedisStreamData> streams = Map.of(
                "s1", mock(RedisStreamData.class),
                "s2", mock(RedisStreamData.class));
        Map<String, StreamId> startIds = Map.of(
                "s1", StreamId.of(0, 1),
                "s2", StreamId.of(9, 9));
        when(streams.get("s1").readNextValues(anyInt(), any()))
                .thenReturn(List.of(mock(StreamValue.class), mock(StreamValue.class)))
                .thenReturn(List.of(mock(StreamValue.class)));
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(20L).thenReturn(120L);

        Future<Map<String, List<StreamValue>>> r = Executors.newFixedThreadPool(1).submit(() -> {
            return StreamsWaitManager.INSTANCE.readWithWait(streams,
                    startIds, 3, clock, 0);
        });

        assertThatExceptionOfType(TimeoutException.class)
                .isThrownBy(() -> r.get(100L, TimeUnit.MILLISECONDS));

        // first notify is for a different streamKey, not in the wait set
        StreamsWaitManager.INSTANCE.addNotify("s3");
        assertThatExceptionOfType(TimeoutException.class)
                .isThrownBy(() -> r.get(10L, TimeUnit.MILLISECONDS));

        // then notify on one in our set and now it wakes up and r.get() will return the result
        StreamsWaitManager.INSTANCE.addNotify("s2");
        Map<String, List<StreamValue>> result = r.get(10L, TimeUnit.MILLISECONDS);
        result = r.get(10L, TimeUnit.MILLISECONDS);

        assertThat(result.keySet()).isEqualTo(Set.of("s1", "s2"));
        assertThat(result.values().stream().collect(Collectors.summingInt(List::size)))
                .isEqualTo(3);

        verify(clock, times(2)).millis();
        verify(streams.get("s1")).readNextValues(3, startIds.get("s1"));
        verify(streams.get("s1")).readNextValues(1, startIds.get("s1"));
        verify(streams.get("s2")).readNextValues(3, startIds.get("s2"));
        verify(streams.get("s2")).readNextValues(1, startIds.get("s2"));
        verifyNoMoreInteractions(clock, streams.get("s1"), streams.get("s2"));
    }

}
