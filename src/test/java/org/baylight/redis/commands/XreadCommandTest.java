package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
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
import org.junit.jupiter.api.Test;

public class XreadCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() throws Exception {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xread(anyList(), anyList(), anyLong()))
                .thenReturn(new ArrayList<>());
        XreadCommand command = new XreadCommand(List.of("key"), List.of("123-*"), null);

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.NULL);
        verify(service).xread(List.of("key"), List.of("123-*"), null);
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
                new RespBulkString("itemId".getBytes())
        };
        XreadCommand command = new XreadCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKeys()).isEqualTo(List.of("key"));
        assertThat(command.getStartValues()).isEqualTo(List.of("itemId"));
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
                .withMessage("XREAD: missing streams arg");

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
