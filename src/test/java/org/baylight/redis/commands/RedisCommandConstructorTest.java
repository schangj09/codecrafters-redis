package org.baylight.redis.commands;

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.io.IOException;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.junit.jupiter.api.Test;

public class RedisCommandConstructorTest implements WithAssertions {

    // Constructs a Ping command.
    @Test
    public void test_newCommandFromValue_returnsPingCommand() throws IOException {
        RespValue value;
        RedisCommand command;
        // given
        value = new RespSimpleStringValue("ping");
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).asInstanceOf(type(PingCommand.class));

        // given
        value = new RespArrayValue(new RespValue[] { new RespSimpleStringValue("PinG"),
                new RespSimpleStringValue("command") });
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).asInstanceOf(type(PingCommand.class));

        // given
        value = new RespBulkString("Ping".getBytes());
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).asInstanceOf(type(PingCommand.class));
    }

    // Constructs a Echo command.
    @Test
    public void test_newCommandFromValue_returnsEchoCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] { new RespSimpleStringValue("echo"),
                new RespSimpleStringValue("command") });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(EchoCommand.class)).matches(
                cmd -> cmd.getEchoValue().equals(new RespBulkString("command".getBytes())),
                "Unexpected value for command: " + actualCommand);
    }

    // Constructs a default Info command.
    @Test
    public void test_newCommandFromValue_returnsInfoCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] { new RespSimpleStringValue("info") });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(InfoCommand.class));
    }

    // Constructs a default Psync command.
    @Test
    public void test_newCommandFromValue_returnsPsyncCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("psync"),
                new RespSimpleStringValue("?"),
                new RespInteger(-1L) });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        String expectedResponse = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        assertThat(actualCommand).asInstanceOf(type(PsyncCommand.class));
        assertThat(actualCommand.asCommand()).isEqualTo(expectedResponse.getBytes());
    }
    // Constructs a default ReplConf command.
    @Test
    public void test_newCommandFromValue_returnsReplConfCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("replconf"),
                new RespSimpleStringValue("listening-port"),
                new RespInteger(123L) });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        String expectedResponse = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$3\r\n123\r\n";
        assertThat(actualCommand).asInstanceOf(type(ReplConfCommand.class));
        assertThat(actualCommand.asCommand()).isEqualTo(expectedResponse.getBytes());
    }

    // Constructs a Get command.
    @Test
    public void test_newCommandFromValue_returnsGetCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] { new RespSimpleStringValue("get"),
                new RespSimpleStringValue("happy") });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(GetCommand.class)).matches(
                cmd -> cmd.getKey().equals(new RespBulkString("happy".getBytes())),
                "Unexpected key for command: " + actualCommand);
        ;
    }

    // Constructs a Set command.
    @Test
    public void test_newCommandFromValue_returnsSetCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] { new RespSimpleStringValue("SET"),
                new RespSimpleStringValue("happy"), new RespBulkString("face".getBytes()) });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(SetCommand.class))
                .matches(cmd -> cmd.getKey().equals(new RespBulkString("happy".getBytes())),
                        "Unexpected key for command: " + actualCommand)
                .matches(cmd -> cmd.getValue().equals(new RespBulkString("face".getBytes())),
                        "Unexpected value for command: " + actualCommand);
    }

    // Returns null if the command type is null.
    @Test
    public void test_newCommandFromValueUnknownValue() throws IOException {
        RespValue value;
        RedisCommand command;
        // given
        value = new RespBulkString("nocommand".getBytes());
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).isNull();

        // given
        value = new RespArrayValue(new RespValue[] { new RespBulkString("nocommand".getBytes()),
                new RespSimpleStringValue("command") });
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).isNull();

        // given
        value = new RespArrayValue(
                new RespValue[] { new RespInteger(123L), new RespSimpleStringValue("ping") });
        // when
        command = new RedisCommandConstructor().newCommandFromValue(value);
        // then
        assertThat(command).isNull();
    }

}