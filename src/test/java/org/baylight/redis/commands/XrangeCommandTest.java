package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.anyString;
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
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.StreamId;
import org.baylight.redis.streams.StreamValue;
import org.junit.jupiter.api.Test;

public class XrangeCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() throws Exception {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xrange(anyString(), anyString(), anyString()))
                .thenReturn(List.of(
                        new StreamValue(StreamId.of(2, 3),
                                new RespValue[] { new RespSimpleStringValue("v1") }),
                        new StreamValue(StreamId.of(3, 4),
                                new RespValue[] { new RespSimpleStringValue("v2") })));
        XrangeCommand command = new XrangeCommand("key", "123-1", "125");

        // when
        byte[] result = command.execute(service);

        // then
        String expectedResult = encodeResponse(
                "*2\r\n*2\r\n$3\r\n2-3\r\n*1\r\n+v1\r\n*2\r\n$3\r\n3-4\r\n*1\r\n+v2\r\n");
        assertThat(encodeResponse(result)).isEqualTo(expectedResult);
        verify(service).xrange("key", "123-1", "125");
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid data, the data should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = { new RespSimpleStringValue("XRANGE"),
                new RespBulkString("key".getBytes()),
                new RespBulkString("startItem".getBytes()),
                new RespBulkString("endItem".getBytes())
        };
        XrangeCommand command = new XrangeCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKey()).isEqualTo("key");
        assertThat(command.getStart()).isEqualTo("startItem");
        assertThat(command.getEnd()).isEqualTo("endItem");
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        List<RespValue> args = new ArrayList<>();
        XrangeCommand command = new XrangeCommand();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XRANGE: Invalid arg, expected string. 0: null");

        args.add(new RespSimpleStringValue("xrange"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XRANGE: Invalid arg, expected string. 1: null");

        args.add(new RespSimpleStringValue("streamKey"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XRANGE: Invalid arg, expected string. 2: null");

        args.add(new RespSimpleStringValue("startItem"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args.toArray(new RespValue[0])))
                .withMessage("XRANGE: Invalid arg, expected string. 3: null");
    }

}