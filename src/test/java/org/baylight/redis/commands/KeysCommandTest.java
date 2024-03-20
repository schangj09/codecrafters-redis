package org.baylight.redis.commands;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

import org.junit.jupiter.api.Test;

public class KeysCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validParamProvided_executeMethodShouldReturnValue() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.getKeys()).thenReturn(List.of("hello", "you"));
        KeysCommand command = new KeysCommand("*");

        // when
        byte[] result = command.execute(service);

        // then
        String expectedResult = encodeResponse("*2\r\n$5\r\nhello\r\n$3\r\nyou\r\n");
        assertThat(encodeResponse(result)).isEqualTo(expectedResult);
        verify(service).getKeys();
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = {};
        KeysCommand command = new KeysCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("KEYS: Missing required arg '' at index 0");
    }

    // When setArgs is called with an missing param, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithMissingParam_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("value") };
        KeysCommand command = new KeysCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("KEYS: Missing required arg '' at index 1");
    }

}