package org.baylight.redis.commands;

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.io.IOException;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueContext;
import org.junit.jupiter.api.Test;

public class RedisCommandConstructorTest implements WithAssertions {
    private static final long START_OFFSET = 345L;

    // Constructs a Config command.
    @Test
    public void test_newCommandFromValue_returnsConfigCommand() {
        // given
        RespArrayValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("config"),
                new RespSimpleStringValue("get"),
                new RespSimpleStringValue("port")
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        String expectedResponse = "*3\r\n$6\r\nconfig\r\n$3\r\nget\r\n$4\r\nport\r\n";
        assertThat(actualCommand).asInstanceOf(type(ConfigCommand.class));
        assertThat(new String(actualCommand.asCommand())).isEqualTo(expectedResponse);
    }

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
        value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("PinG"),
                new RespSimpleStringValue("command")
        });
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
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("echo"),
                new RespSimpleStringValue("command")
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(EchoCommand.class)).matches(cmd -> {
            assertThat(cmd.getEchoValue()).isEqualTo(new RespBulkString("command".getBytes()));
            return true;
        });
    }

    // Constructs a default Info command.
    @Test
    public void test_newCommandFromValue_returnsInfoCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("info")
        });

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
                new RespSimpleStringValue("?"), new RespInteger(-1L)
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        String expectedResponse = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        assertThat(actualCommand).asInstanceOf(type(PsyncCommand.class));
        assertThat(new String(actualCommand.asCommand())).isEqualTo(expectedResponse);
    }

    // Constructs a default ReplConf command.
    @Test
    public void test_newCommandFromValue_returnsReplConfCommand() {
        // given
        RespArrayValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("replconf"),
                new RespSimpleStringValue("listening-port"),
                new RespInteger(123L)
        });
        value.setContext(new RespValueContext(START_OFFSET, 21));

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        String expectedResponse = "*3\r\n$8\r\nreplconf\r\n$14\r\nlistening-port\r\n$3\r\n123\r\n";
        assertThat(actualCommand).asInstanceOf(type(ReplConfCommand.class));
        assertThat(new String(actualCommand.asCommand())).isEqualTo(expectedResponse);
        assertThat(actualCommand).hasFieldOrPropertyWithValue("startBytesOffset", START_OFFSET);
    }

    // Constructs a Get command.
    @Test
    public void test_newCommandFromValue_returnsGetCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("get"),
                new RespSimpleStringValue("happy")
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(GetCommand.class))
                .matches(cmd -> {
                    assertThat(cmd.getKey()).isEqualTo(new RespBulkString("happy".getBytes()));
                    return true;
                });
        ;
    }

    // Constructs a Set command.
    @Test
    public void test_newCommandFromValue_returnsSetCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("SET"),
                new RespSimpleStringValue("happy"),
                new RespBulkString("face".getBytes())
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(SetCommand.class))
                .matches(cmd -> {
                    assertThat(cmd.getKey()).isEqualTo(new RespBulkString("happy".getBytes()));
                    assertThat(cmd.getValue()).isEqualTo(new RespBulkString("face".getBytes()));
                    return true;
                });
    }

    // Constructs a Wait command.
    @Test
    public void test_newCommandFromValue_returnsWaitCommand() {
        // given
        RespValue value = new RespArrayValue(new RespValue[] {
                new RespSimpleStringValue("WAIT"),
                new RespSimpleStringValue("3"),
                new RespBulkString("456".getBytes())
        });

        // when
        RedisCommand actualCommand = new RedisCommandConstructor().newCommandFromValue(value);

        // then
        assertThat(actualCommand).asInstanceOf(type(WaitCommand.class))
                .matches(cmd -> {
                    assertThat(cmd.getNumReplicas()).isEqualTo(3);
                    assertThat(cmd.getTimeoutMillis()).isEqualTo(456L);
                    return true;
                });
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
        value = new RespArrayValue(new RespValue[] {
                new RespBulkString("nocommand".getBytes()),
                new RespSimpleStringValue("command")
        });
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