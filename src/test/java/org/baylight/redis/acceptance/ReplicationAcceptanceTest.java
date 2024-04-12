package org.baylight.redis.acceptance;

import java.net.Socket;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.ClientConnection;
import org.baylight.redis.ServiceRunner;
import org.baylight.redis.TestConstants;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.protocol.RespValueParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ReplicationAcceptanceTest implements WithAssertions, TestConstants {
    private static ServiceRunner leader;
    private static ServiceRunner follower1;
    private static ServiceRunner follower2;

    @BeforeAll
    static void setUp() throws Exception {
        leader = new ServiceRunner();
        new Thread(leader).start();

        follower1 = new ServiceRunner("--replicaof", "localhost", "6379", "--port", "6380");
        new Thread(follower1).start();

        follower2 = new ServiceRunner("--replicaof", "localhost", "6379", "--port", "6381");
        new Thread(follower2).start();

        // allow the replicas complete the handshake before running tests
        Thread.sleep(500);
    }

    @AfterAll
    static void tearDown() {
        follower2.terminate();
        follower1.terminate();
        // stop the leader server
        leader.terminate();
    }

    @Test
    void testSetAndGetReplicated() throws Exception {
        Socket socket = new Socket("localhost", 6379);
        Socket f1Socket = new Socket("localhost", 6380);
        Socket f2Socket = new Socket("localhost", 6381);

        try {
            ClientConnection conn = new ClientConnection(socket, new RespValueParser());
            ClientConnection conn1 = new ClientConnection(f1Socket, new RespValueParser());
            ClientConnection conn2 = new ClientConnection(f2Socket, new RespValueParser());
            RespValue value;

            conn.writeFlush("*2\r\n+get\r\n+m1\r\n".getBytes());
            value = conn.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse("$-1\r\n"));

            conn.writeFlush("*3\r\n+set\r\n+m1\r\n+456\r\n".getBytes());
            value = conn.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse("+OK\r\n"));

            conn1.writeFlush("*2\r\n+get\r\n+m1\r\n".getBytes());
            value = conn1.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse("$3\r\n456\r\n"));

            conn2.writeFlush("*2\r\n+get\r\n+m1\r\n".getBytes());
            value = conn2.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse("$3\r\n456\r\n"));
        } finally {
            socket.close();
            f1Socket.close();
            f2Socket.close();
        }
    }

    @Test
    void testWaitForReplConfAck() throws Exception {
        Socket socket = new Socket("localhost", 6379);
        Socket f1Socket = new Socket("localhost", 6380);
        Socket f2Socket = new Socket("localhost", 6381);

        try {
            ClientConnection conn = new ClientConnection(socket, new RespValueParser());
            RespValue value;

            conn.writeFlush("*3\r\n+set\r\n+m2\r\n+456\r\n".getBytes());
            value = conn.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse("+OK\r\n"));

            conn.writeFlush("*3\r\n+wait\r\n+4\r\n+1000\r\n".getBytes());
            value = conn.readValue();
            System.out.println(value.toString());
            assertThat(encodeResponse(value)).isEqualTo(encodeResponse(":2\r\n"));
        } finally {
            socket.close();
            f1Socket.close();
            f2Socket.close();
        }
    }
}
