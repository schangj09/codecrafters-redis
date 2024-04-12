package org.baylight.redis.acceptance;

import java.net.Socket;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.ClientConnection;
import org.baylight.redis.RedisServiceOptions;
import org.baylight.redis.ServiceRunner;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class BaseAcceptanceTest implements WithAssertions, TestConstants {
    private static ServiceRunner leader;

    @BeforeAll
    static void setUp() {
        leader = new ServiceRunner(new RedisServiceOptions());
        new Thread(leader).start();
    }

    @AfterAll
    static void tearDown() {
        // stop the leader server
        leader.terminate();
    }

    @Test
    void testPing() throws Exception {
        Socket socket = new Socket("localhost", 6379);

        ClientConnection conn = new ClientConnection(socket, new RespValueParser());
        conn.writeFlush("*1\r\n+ping\r\n".getBytes());
        RespValue value = conn.readValue();
        System.out.println(value.toString());
        assertThat(encodeResponse(value)).isEqualTo(encodeResponse("+PONG\r\n"));
    }

    @Test
    void testSetGet() throws Exception {
        Socket socket = new Socket("localhost", 6379);

        ClientConnection conn = new ClientConnection(socket, new RespValueParser());
        RespValue value;

        conn.writeFlush("*2\r\n+get\r\n+m1\r\n".getBytes());
        value = conn.readValue();
        System.out.println(value.toString());
        assertThat(encodeResponse(value)).isEqualTo(encodeResponse("$-1\r\n"));

        conn.writeFlush("*3\r\n+set\r\n+m1\r\n+456\r\n".getBytes());
        value = conn.readValue();
        System.out.println(value.toString());
        assertThat(encodeResponse(value)).isEqualTo(encodeResponse("+OK\r\n"));

        conn.writeFlush("*2\r\n+get\r\n+m1\r\n".getBytes());
        value = conn.readValue();
        System.out.println(value.toString());
        assertThat(encodeResponse(value)).isEqualTo(encodeResponse("$3\r\n456\r\n"));
    }
}
