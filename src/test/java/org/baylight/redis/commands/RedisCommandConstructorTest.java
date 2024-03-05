package org.baylight.redis.commands;

import org.assertj.core.api.WithAssertions;


public class RedisCommandConstructorTest implements WithAssertions {
/*
    // Constructs a Ping command.
    @Test
    public void test_newCommandFromValue_returnsPingCommand() throws IOException {
        // given
        RespValue value = new RespSimpleStringValue("ping");
        RedisCommand expectedCommand = new PingCommand();
    
        // when
        RedisCommand actualCommand = RedisCommandConstructor.newCommandFromValue(value);
    
        // then
        assertThat(actualCommand).isEqualTo(expectedCommand);
    }

    // Constructs a Echo command.
    @Test
    public void test_newCommandFromValue_returnsEchoCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
            new RespSimpleStringValue("echo"),
            new RespSimpleStringValue("command")
        });
        RedisCommand expectedCommand = new EchoCommand(new RespBulkString("command".getBytes()));

        // when
        RedisCommand actualCommand = RedisCommandConstructor.newCommandFromValue(value);
    
        // then
        assertThat(actualCommand).isEqualTo(expectedCommand);
    }

    // Constructs a default Info command.
    @Test
    public void test_newCommandFromValue_returnsInfoCommand() {
    }

    // Constructs a Get command.
    @Test
    public void test_newCommandFromValue_returnsGetCommand() {
    }

    // Constructs a Get command.
    @Test
    public void test_newCommandFromValue_returnsSetCommand() {
    }

    // Returns null if the command type is null.
    @Test
    public void test_newCommandFromValueUnknownValue() throws IOException {
        RespValue value;
        RedisCommand command;
        // given
        value = new RespBulkString("nocommand".getBytes());
        // when
        command = RedisCommandConstructor.newCommandFromValue(value);
        // then
        assertThat(command).isNull();

        // given
        value = new RespArrayValue(new RespValue[] {
            new RespBulkString("nocommand".getBytes()),
            new RespSimpleStringValue("command")
        });
        // when
        command = RedisCommandConstructor.newCommandFromValue(value);
        // then
        assertThat(command).isNull();

        // given
        value = new RespArrayValue(new RespValue[] {
            new RespInteger(123L),
            new RespSimpleStringValue("ping")
        });
        // when
        command = RedisCommandConstructor.newCommandFromValue(value);
        // then
        assertThat(command).isNull();
    }
*/
}