package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.StreamId;
import org.baylight.redis.streams.StreamValue;
import org.junit.jupiter.api.Test;

public class XreadCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() throws Exception {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xread(anyList(), anyList(), any()))
                .thenReturn(
                        List.of(
                                List.of(new StreamValue(StreamId.of(0, 1), new RespValue[] {})),
                                List.of(new StreamValue(StreamId.of(1, 1), new RespValue[] {}))));
        XreadCommand command = new XreadCommand(List.of("key", "k2"), List.of("123-*", "0-0"),
                null);

        // when
        byte[] result = command.execute(service);

        // then
        String expected = encodeResponse(
                "*2\r\n*2\r\n+key\r\n*1\r\n*2\r\n$3\r\n0-1\r\n*0\r\n*2\r\n+k2\r\n*1\r\n*2\r\n$3\r\n1-1\r\n*0\r\n");
        assertThat(encodeResponse(result)).isEqualTo(expected);
        verify(service).xread(List.of("key", "k2"), List.of("123-*", "0-0"), null);
        verifyNoMoreInteractions(service);
    }

    // When a timeout is provided, the execute method should return NULL in case of no results
    @Test
    public void test_validKeyProvided_executeMethodReturnsNullForTimeout() throws Exception {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xread(anyList(), anyList(), any()))
                .thenReturn(List.of(List.of(), List.of()));
        XreadCommand command = new XreadCommand(List.of("key", "k2"), List.of("123-*", "0-0"),
                1L);

        // when
        byte[] result = command.execute(service);

        // then
        String expected = encodeResponse(RespConstants.NULL);
        assertThat(encodeResponse(result)).isEqualTo(expected);
        verify(service).xread(List.of("key", "k2"), List.of("123-*", "0-0"), 1L);
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid data, the data should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = {
                new RespSimpleStringValue("XREAD"),
                new RespBulkString("streams".getBytes()),
                new RespBulkString("key".getBytes()),
                new RespBulkString("key2".getBytes()),
                new RespBulkString("itemId".getBytes()),
                new RespBulkString("itemId2".getBytes())
        };
        XreadCommand command = new XreadCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKeys()).isEqualTo(List.of("key", "key2"));
        assertThat(command.getStartValues()).isEqualTo(List.of("itemId", "itemId2"));
        assertThat(command.getTimeoutMillis()).isNull();
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        List<RespValue> args = new ArrayList<>();
        XreadCommand command = new XreadCommand();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XREAD: Missing required arg '' at index 0");

        args.add(new RespSimpleStringValue("xread"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XREAD: Missing streams arg");

        args.add(new RespSimpleStringValue("streams"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XREAD: Invalid number of streams pairs");

        args.add(new RespSimpleStringValue("key"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XREAD: Invalid number of streams pairs");
    }

}
