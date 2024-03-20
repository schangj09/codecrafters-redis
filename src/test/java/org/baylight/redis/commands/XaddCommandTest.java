package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.StreamId;

import org.junit.jupiter.api.Test;

public class XaddCommandTest implements WithAssertions, TestConstants {

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validKeyProvided_executeMethodShouldReturnValue() throws Exception {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        when(service.xadd(anyString(), anyString(), any()))
                .thenReturn(new StreamId(123L, 110));
        XaddCommand command = new XaddCommand("key", "123-*");

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(new RespBulkString("123-110".getBytes()).asResponse());
        verify(service).xadd("key", "123-*", new RespValue[0]);
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid data, the data should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = { new RespSimpleStringValue("XADD"),
                new RespBulkString("key".getBytes()),
                new RespBulkString("itemId".getBytes()),
                new RespBulkString("i1".getBytes()),
                new RespBulkString("val1".getBytes()),
                new RespBulkString("i2".getBytes()),
                new RespBulkString("val2".getBytes())
        };
        XaddCommand command = new XaddCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.getKey()).isEqualTo("key");
        assertThat(command.getItemId()).isEqualTo("itemId");
        assertThat(command.getItemMap()).isEqualTo(new RespValue[] {
                new RespBulkString("i1".getBytes()),
                new RespBulkString("val1".getBytes()),
                new RespBulkString("i2".getBytes()),
                new RespBulkString("val2".getBytes())
        });
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

    // When setArgs is called with invalid data, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithWrongNumber_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("XADD"),
                new RespBulkString("key".getBytes()),
                new RespBulkString("itemId".getBytes()),
                new RespBulkString("i1".getBytes())
        };
        XaddCommand command = new XaddCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("XADD: Invalid number of item key value pairs");
    }

    // When setArgs is called with duplicate data, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithDuplicate_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("XADD"),
                new RespBulkString("key".getBytes()),
                new RespBulkString("itemId".getBytes()),
                new RespBulkString("i2".getBytes()),
                new RespBulkString("val1".getBytes()),
                new RespBulkString("i2".getBytes()),
                new RespBulkString("val2".getBytes())
        };
        XaddCommand command = new XaddCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("XADD: Duplicate item key: BulkString [length=2, value=i2]");
    }

}