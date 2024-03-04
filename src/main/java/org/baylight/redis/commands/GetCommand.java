package org.baylight.redis.commands;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.StoredData;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;
import java.util.Map;

public class GetCommand extends RedisCommand {
    private RespBulkString key;

    public GetCommand() {
        super(Type.GET);
    }

    public GetCommand(RespBulkString key) {
        super(Type.GET);
        this.key = key;
    }

	public RespBulkString getKey() {
		return key;
	}

    @Override
    public void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] {
                ":string", // command name
                ":string" // key
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        this.key = optionsMap.get("1").asBulkString();
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        if (service.containsKey(key.getValueAsString())) {
            StoredData storedData = service.get(key.getValueAsString());
            if (service.isExpired(storedData)) {
                service.delete(key.getValueAsString());
                return RespConstants.NULL;
            }
            return new RespBulkString(storedData.getValue()).asResponse();
        }
        return RespConstants.NULL;
    }

    @Override
    public String toString() {
        return "GetCommand [key=" + key + "]";
    }

}
