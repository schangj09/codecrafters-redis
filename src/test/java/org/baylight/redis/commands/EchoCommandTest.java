package org.baylight.redis.commands;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.LeaderService;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.testng.annotations.Test;

public class EchoCommandTest implements WithAssertions {
    // EchoCommand object can be created successfully with no arguments
    @Test
    public void testEchoCommandCreatedWithNoArguments() {
        EchoCommand echoCommand = new EchoCommand();
        assertThat(echoCommand).isNotNull();
        assertThat(echoCommand.getType()).isEqualTo(RedisCommand.Type.ECHO);
        assertThat(echoCommand.bulkStringArg).isNull();
    }

    // When a valid key is provided, the execute method should return the value associated with the
    // key.
    @Test
    public void test_validValueProvided_executeMethodShouldReturnValue() {
        // given
        RedisServiceBase service = mock(LeaderService.class);
        RespBulkString bulkStringArg = new RespBulkString("test".getBytes());
        EchoCommand command = new EchoCommand(bulkStringArg);

        // when
        byte[] result = command.execute(service);

        // then
        assertThat(result).isEqualTo(new RespBulkString("test".getBytes()).asResponse());
        verifyNoMoreInteractions(service);
    }

    // When setArgs is called with a valid key, the key should be correctly set.
    @Test
    public void test_setArgsCalledWithValidKey_keyShouldBeCorrectlySet() {
        // given
        RespValue[] args = { new RespSimpleStringValue("GET"),
                new RespBulkString("key".getBytes()) };
        EchoCommand command = new EchoCommand();

        // when
        command.setArgs(args);

        // then
        assertThat(command.bulkStringArg.getValueAsString()).isEqualTo("key");
    }

    // When setArgs is called with no arguments, an IllegalArgumentException should be thrown.
    @Test
    public void test_setArgsCalledWithNoArguments_illegalArgumentExceptionShouldBeThrown() {
        // given
        RespValue[] args = { new RespSimpleStringValue("echo") };
        EchoCommand command = new EchoCommand();

        // when
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> command.setArgs(args))
                .withMessage("ECHO: Invalid number of arguments: 1");
    }

}
