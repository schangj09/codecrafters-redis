package org.baylight.redis.commands;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespBulkString;
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
    
    @Test
    public void testEchoCommandObjectThrowsExceptionWithInvalidArgumentType() {
        RespBulkString bulkStringArg = new RespBulkString("test".getBytes());
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new EchoCommand(bulkStringArg))
                .withMessage("Invalid argument type");
    }

}
