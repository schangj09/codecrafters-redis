package org.baylight.redis.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.protocol.RespArrayValue;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleErrorValue;
import org.baylight.redis.protocol.RespValue;
import org.baylight.redis.streams.IllegalStreamItemIdException;
import org.baylight.redis.streams.StreamValue;

public class XreadCommand extends RedisCommand {
    private List<String> keys;
    private List<String> startValues;
    private Long timeoutMillis;

    public XreadCommand() {
        super(Type.XREAD);
        keys = new ArrayList<>();
        startValues = new ArrayList<>();
        timeoutMillis = null;
    }

    public XreadCommand(List<String> keys, List<String> startValues, Long timeoutMillis) {
        super(Type.XREAD);
        this.keys = keys;
        this.startValues = startValues;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public boolean isBlockingCommand() {
        return timeoutMillis != null;
    }

    @Override
    public byte[] execute(RedisServiceBase service) {
        try {
            List<List<StreamValue>> result = service.xread(keys, startValues, timeoutMillis);
            if (timeoutMillis != null) {
                // special case if there are no results and timeout was specified, then we need to
                // return null instead of the empty lists
                if (result.stream().collect(Collectors.summingInt(List::size)) == 0) {
                    return RespConstants.NULL;
                }
            }
            List<List<RespValue>> resultResp = new ArrayList<>();
            for (int i = 0; i < keys.size(); i++) {
                List<StreamValue> values = result.get(i);
                List<RespValue> respValuesForKey = new ArrayList<>();
                respValuesForKey.add(RespValue.simpleString(keys.get(i)));
                respValuesForKey.add(RespValue.array(values.stream()
                        .map(StreamValue::asRespArrayValue).toArray(RespArrayValue[]::new)));
                resultResp.add(respValuesForKey);
            }
            return RespValue.array(
                    resultResp.stream().map(RespValue::array).toArray(RespValue[]::new))
                    .asResponse();
        } catch (

        IllegalStreamItemIdException e) {
            return new RespSimpleErrorValue(e.getMessage()).asResponse();
        }
    }

    @Override
    public byte[] asCommand() {
        return new RespArrayValue(
                new RespValue[] {
                        new RespBulkString(getType().name().getBytes()),
                        new RespBulkString("streams".getBytes())
                }).asResponse();
    }

    @Override
    protected void setArgs(RespValue[] args) {
        ArgReader argReader = new ArgReader(type.name(), new String[] { ":string", // command name
                "[block:int]",
                "[streams:var]" // streams key with variable args after it
        });
        Map<String, RespValue> optionsMap = argReader.readArgs(args);
        if (optionsMap.containsKey("block")) {
            timeoutMillis = optionsMap.get("block").getValueAsLong();
        }
        if (!optionsMap.containsKey("streams")) {
            throw new IllegalArgumentException(String
                    .format("%s: missing streams arg", type.name()));
        } else {
            RespArrayValue streams = (RespArrayValue) optionsMap.get("streams");
            RespValue[] valuesArray = streams.getValues();
            if (valuesArray.length == 0 || valuesArray.length % 2 == 1) {
                throw new IllegalArgumentException(
                        String.format("%s: Invalid number of streams pairs", type.name()));
            }
            int n = valuesArray.length / 2;
            for (int i = 0; i < n; i++) {
                keys.add(valuesArray[i].getValueAsString());
                startValues.add(valuesArray[n + i].getValueAsString());
            }
        }
    }

    @Override
    public String toString() {
        return "XreadCommand [keys=" + keys + ", startValues=" + startValues + ", timeoutMillis="
                + timeoutMillis + "]";
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<String> getStartValues() {
        return startValues;
    }

    public Long getTimeoutMillis() {
        return timeoutMillis;
    }

}
