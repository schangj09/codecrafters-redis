package org.baylight.redis.commands;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespSimpleErrorValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.IllegalStreamItemIdException;
import org.baylight.redis.streams.StreamId;

public class XaddCommand extends RedisCommand {
    private String key;
    private String itemId;
    RespValue[] itemMap = null;

    public XaddCommand() {
        super(Type.XADD);
    }

    public XaddCommand(String key, String itemId) {
        super(Type.XADD);
        this.key = key;
        this.itemId = itemId;
        this.itemMap = new RespValue[0];
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        try {
            StreamId streamId = service.xadd(key, itemId, itemMap);
            return new RespBulkString(streamId.getId().getBytes()).asResponse();
        } catch (IllegalStreamItemIdException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] { new RespBulkString(getType().name().getBytes()) }).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        validateArgIsString(args, 0);
        if (!args[0].getValueAsString().toLowerCase().equals("xadd")) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid command arg: %s", type.name(),
                            args[0].getValueAsString()));
        }
        validateArgIsString(args, 1);
        key = args[1].getValueAsString();
        validateArgIsString(args, 2);
        itemId = args[2].getValueAsString();

        int nextArg = 3;
        itemMap = new RespValue[args.length - nextArg];
        int itemIndex = 0;
        Set<RespValue> itemKeys = new HashSet<>();
        if ((args.length - nextArg) % 2 == 1) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid number of item key value pairs", type.name()));
        }
        for (int i = nextArg; i < args.length; i += 2, itemIndex += 2) {
            validateArgIsString(args, i);
            validateArgIsString(args, i + 1);
            if (itemKeys.contains(args[i])) {
                throw new IllegalArgumentException(
                        String.format("%s: Duplicate item key: %s", type.name(), args[i]));
            }
            itemKeys.add(args[i]);
            itemMap[itemIndex] = args[i];
            itemMap[itemIndex + 1] = args[i + 1];
        }
    }

    @Override
    public String toString() {
        return "XaddCommand [key=" + key + ", itemId=" + itemId + ", itemMap="
                + Arrays.toString(itemMap) + "]";
    }

    public String getKey() {
        return key;
    }

    public String getItemId() {
        return itemId;
    }

    public RespValue[] getItemMap() {
        return itemMap;
    }

}
