package org.baylight.redis.commands;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.baylight.redis.RedisService;
import org.baylight.redis.StoredData;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

public class SetCommand extends RedisCommand {

    // args state machine
    // 1 key
    // 2 value
    // 3 [NX | XX]
    // 4 [GET]
    // 5 [EX | PX | EXAT | PXATT]
    // 6 [KEEPTTL]
    // 7 [INT_VALUE]
    // 8 term
    private static final Map<Integer, List<String>> states = Map.of(
            1, List.of("key"),
            2, List.of("value"),
            3, List.of("nx", "xx"),
            4, List.of("get"),
            5, List.of("ex", "px", "exat", "pxatt"),
            6, List.of("keepttl"),
            7, List.of("int_value"));

    private static final Map<Integer, List<Integer>> transitions = Map.of(
            1, List.of(2),
            2, List.of(3, 4, 5, 6, 8),
            3, List.of(4, 5, 6, 8),
            4, List.of(5, 6, 8),
            5, List.of(7),
            6, List.of(8),
            7, List.of(8));
    private static final int TERMINAL = 8;
    private static final int INT_VALUE = 7;

    Map<String, Boolean> optionsMap = new HashMap<>();
    Long ttlValue = null;

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
        validateNumArgs(args, len -> len >= 3);
        validateArgIsString(args, 1);
        validateArgIsString(args, 2);
        this.key = args[1].asBulkString();
        this.value = args[2].asBulkString();
        int state = 2;
        int i = 3;
        while (i <= args.length && state < TERMINAL) {
            int nextState = (i == args.length) ? TERMINAL : findState(args[i]);
            validateArgForStateTransition(args, i, state, nextState, transitions);
            if (nextState == INT_VALUE) {
                validateArgIsInteger(args, i);
                ttlValue = args[i].getValueAsLong();
            } else if (nextState != TERMINAL) {
                String option = args[i].getValueAsString().toLowerCase();
                optionsMap.put(option, true);
            }
            i++;
            state = nextState;
        }
    }

    private int findState(RespValue arg) {
        return findStateForArg(arg, (a) -> a.isInteger() ? "int_value" : a.getValueAsString().toLowerCase(), states);
    }

    int findStateForArg(RespValue arg, Function<RespValue, String> argNameMapper, Map<Integer, List<String>> states) {
        String argValue = argNameMapper.apply(arg);
        for (Integer state : states.keySet()) {
            if (states.get(state).contains(argValue)) {
                return state;
            }
        }
        return -1;
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

        Long ttl = ttlValue;
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

    @Override
    public String toString() {
        return "SetCommand [key=" + key + ", value=" + value + "]";
    }
}
