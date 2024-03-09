package org.baylight.redis.commands;

import java.util.Map;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespInteger;
import org.baylight.redis.protocol.RespValue;

public class WaitCommand extends RedisCommand {
    int numReplicas;
    long timeoutMillis;

    private static ArgReader ARG_READER = new ArgReader(
        Type.WAIT.name(),
        new String[] { ":string", // command name
                ":int", // number of replicas to wait for
                ":int" // timeout in milliseconds
        });

    public WaitCommand() {
        super(Type.WAIT);
    }

    public WaitCommand(int timeout, int numReplicas) {
        super(Type.WAIT);
    }

    /**
     * @return the numReplicas
     */
    public int getNumReplicas() {
        return numReplicas;
    }

    /**
     * @return the timeoutMillis
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        return new RespInteger(numReplicas).asResponse();
    }

    @Override
    public void setArgs(RespValue[] args) {
        Map<String, RespValue> argsMap = ARG_READER.readArgs(args);
        numReplicas = argsMap.get("1").getValueAsLong().intValue();
        timeoutMillis = argsMap.get("2").getValueAsLong();
    }

    @Override
    public String toString() {
        return "WaitCommand [numReplicas=" + numReplicas + ", timeoutMillis=" + timeoutMillis + "]";
    }

}
