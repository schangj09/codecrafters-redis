package org.baylight.redis.commands;

import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.RedisService;
import org.baylight.redis.StoredData;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public class SetCommand extends RedisCommand {

    // Args reader specification for SET command
    private ArgReader argReader = new ArgReader(type.name(), new String[] {
            ":string", // command name
            ":string", // key
            ":string", // value
            "[nx, xx]",
            "[get]",
            "[ex:int px:int exat:int pxatt:int keepttl]"
    });

    Map<String, RespValue> optionsMap = new HashMap<>();

    RespBulkString key;
    RespBulkString value;

    public SetCommand() {
        super(Type.SET);
    }

    public SetCommand(RespBulkString key, RespBulkString value) {
        super(Type.SET);
        this.key = key;
        this.value = value;
    }

    @Override
    public void setArgs(RespValue[] args) {
        optionsMap = argReader.readArgs(args);
        key = optionsMap.get("1").asBulkString();
        value = optionsMap.get("2").asBulkString();
    }

    @Override
    public byte[] execute(RedisService service) {
        long now = service.getCurrentTime();
        String keyString = key.getValueAsString();

        // only set if it is NOT already stored in the map
        if (optionsMap.containsKey("nx")) {
            if (service.containsUnexpiredKey(keyString)) {
                return RespConstants.NULL;
            }
        }
        // only set if it is already stored in the map
        if (optionsMap.containsKey("xx")) {
            if (!service.containsUnexpiredKey(keyString)) {
                return RespConstants.NULL;
            }
        }
        boolean doKeepTtl = optionsMap.containsKey("keepttl");
        boolean doGet = optionsMap.containsKey("get");

        Long ttl = getTtl(now);
        StoredData prevData = null;
        if ((doGet || doKeepTtl) && service.containsKey(keyString)) {
            prevData = service.get(keyString);
            ttl = doKeepTtl ? prevData.getTtlMillis() : ttl;
        }
        StoredData storedData = new StoredData(value.getValue(), now, ttl);
        service.set(keyString, storedData);
        return (doGet && prevData != null)
                ? new RespBulkString(prevData.getValue()).asResponse()
                : RespConstants.OK;
    }

    private Long getTtl(long now) {
        if (optionsMap.containsKey("ex")) {
            return optionsMap.get("ex").getValueAsLong() * 1000;
        } else if (optionsMap.containsKey("px")) {
            return optionsMap.get("px").getValueAsLong();
        } else if (optionsMap.containsKey("exat")) {
            return optionsMap.get("exat").getValueAsLong() * 1000 - now;
        } else if (optionsMap.containsKey("pxatt")) {
            return optionsMap.get("pxatt").getValueAsLong() - now;
        }
        return null;
    }

    @Override
    public String toString() {
        return "SetCommand [key=" + key + ", value=" + value + "]";
    }
}
