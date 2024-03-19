package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.StoredData;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.RedisStreamData;

// Note: Generated by CodiumAI
// However, I had to do some work to add verifications.

import org.junit.jupiter.api.Test;

public class XaddCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xadd(anyString(), anyString(), anyMap()))
                .thenReturn(new StoredData(new RedisStreamData("key"), CLOCK_MILLIS, null));
        XaddCommand command = new XaddCommand("key", "itemId");

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(new RespBulkString("itemId".getBytes()).asResponse());
        verify(service).xadd("key", "itemId", new HashMap<>());
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid key, the key should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = { new RespSimpleStringValue("XADD"),
                new RespBulkString("key".getBytes()),
                new RespBulkString("itemId".getBytes()) };
        XaddCommand command = new XaddCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKey()).isEqualTo("key");
        assertThat(command.getItemId()).isEqualTo("itemId");
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = {};
        XaddCommand command = new XaddCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("XADD: Invalid arg, expected string. 0: null");
    }

    // When setArgs is called with an invalid key, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithInvalidKey_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("xadd") };
        XaddCommand command = new XaddCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("XADD: Invalid arg, expected string. 1: null");
    }

}