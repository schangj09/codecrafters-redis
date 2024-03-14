package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.commands.ReplConfCommand.Option;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

// Note: Generated by CodiumAI
// However, I had to do some work to add verifications.

import org.junit.jupiter.api.Test;

public class ReplConfCommandTest implements WithAssertions {

    private static final long START_OFFSET = 345L;
    private static final RespSimpleStringValue V1 = new RespSimpleStringValue("REPLCONF");

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_listeningPortProvided_executeMethodShouldConfirmListeningPort() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.replicationConfirm(anyMap(), anyLong())).thenReturn(RespConstants.OK);
        ReplConfCommand command = new ReplConfCommand(Option.LISTENING_PORT, "1111", START_OFFSET);

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.OK);
        verify(service).replicationConfirm(
                eq(Map.of("0", V1, "listening-port", new RespSimpleStringValue("1111"))),
                eq(START_OFFSET));
        verifyNoMoreInteractions(service);
    }

    // When an expired key is provided, the execute method should delete the key and return NULL.
    @Test
    public void test_capaProvided_executeMethodShouldConfirmPsync2() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.replicationConfirm(anyMap(), anyLong())).thenReturn(RespConstants.OK);
        ReplConfCommand command = new ReplConfCommand(Option.CAPA, "psync2", START_OFFSET);

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.OK);
        verify(service).replicationConfirm(
                eq(Map.of("0", V1, "capa", new RespSimpleStringValue("psync2"))), eq(START_OFFSET));
        verifyNoMoreInteractions(service);
    }

    // When a non-existent key is provided, the execute method should return NULL.
    @Test
    public void test_getackProvided_executeMethodShouldConfirmGetack() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.replicationConfirm(anyMap(), anyLong())).thenReturn(RespConstants.OK);
        ReplConfCommand command = new ReplConfCommand(Option.GETACK, "*", START_OFFSET);

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.OK);
        verify(service).replicationConfirm(
                eq(Map.of("0", V1, "getack", new RespSimpleStringValue("*"))), eq(START_OFFSET));
        verifyNoMoreInteractions(service);
    }

    // setArgs for GETACK
    @Test
    public void test_command_setArgsGetack() {
        // given
        ReplConfCommand command = new ReplConfCommand();
        RespValue[] args = { V1, new RespBulkString("GETACK".getBytes()),
                new RespBulkString("*".getBytes()) };

        // when
        command.setArgs(args);

        // then
        assertThat(command.getOptionsMap())
                .isEqualTo(Map.of("0", V1, "getack", new RespBulkString("*".getBytes())));
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = {};
        ReplConfCommand command = new ReplConfCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("REPLCONF: Missing required arg '' at index 0");
    }

}