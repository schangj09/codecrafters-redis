package org.baylight.redis.acceptance;

import java.util.Arrays;

import org.baylight.redis.commands.RedisCommand;
import org.baylight.redis.protocol.RespValue;

public class CommandResponse {
    RedisCommand command;
    RespValue response;
    Long delayMillis;

    public CommandResponse(RedisCommand command, RespValue response) {
        this(command, response, null);
    }

    public CommandResponse(RedisCommand command, RespValue response, Long delayMillis) {
        this.command = command;
        this.response = response;
        this.delayMillis = delayMillis;
    }

    boolean commandMatches(RedisCommand command) {
        return Arrays.compare(this.command.asCommand(), command.asCommand()) == 0;
    }
}
