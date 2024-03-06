package org.baylight.redis.commands;

import static org.baylight.redis.TestConstants.FIXED_CLOCK_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.StoredData;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

// Note: Generated by CodiumAI
// However, I had to do some work to add verifications.

import org.junit.jupiter.api.Test;

public class GetCommandTest implements WithAssertions {


    // When a valid key is provided, the execute method should return the value associated with the key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        StoredData storedData = new StoredData("value".getBytes(), 0, null);
        when(service.containsKey(anyString())).thenReturn(true);
        when(service.get(anyString())).thenReturn(storedData);
        GetCommand command = new GetCommand(new RespBulkString("key".getBytes()));

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(new RespBulkString("value".getBytes()).asResponse());
        verify(service).containsKey("key");
        verify(service).get("key");
        verify(service).isExpired(storedData);
        verifyNoMoreInteractions(service);
    }

    // When an expired key is provided, the execute method should delete the key and return NULL.
    @Test
    public void test_expiredKeyProvided_executeMethodShouldDeleteExpiredKeyAndReturnNull() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        StoredData storedData = new StoredData("value".getBytes(), FIXED_CLOCK_EPOCH.millis(), 100L);
        when(service.containsKey(anyString())).thenReturn(true);
        when(service.get(anyString())).thenReturn(storedData);
        when(service.isExpired(any())).thenReturn(storedData.isExpired(101L));
        GetCommand command = new GetCommand(new RespBulkString("key".getBytes()));

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.NULL);
        verify(service).containsKey("key");
        verify(service).get("key");
        verify(service).isExpired(storedData);
        verify(service).delete("key");
        verifyNoMoreInteractions(service);
    }

    // When a non-existent key is provided, the execute method should return NULL.
    @Test
    public void test_nonExistentKeyProvided_executeMethodShouldReturnNull() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.containsKey(anyString())).thenReturn(false);
        GetCommand command = new GetCommand(new RespBulkString("key".getBytes()));

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(RespConstants.NULL);
        verify(service).containsKey("key");
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid key, the key should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = { new RespSimpleStringValue("GET"), new RespBulkString("key".getBytes()) };
        GetCommand command = new GetCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKey().getValueAsString()).isEqualTo("key");
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = {};
        GetCommand command = new GetCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("GET: Missing required arg '' at index 0");
    }

    // When setArgs is called with an invalid key, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithInvalidKey_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("value") };
        GetCommand command = new GetCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("GET: Missing required arg '' at index 1");
    }

}