package org.baylight.redis.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

// Arg Spec is space separated list of required args followed by option groups
// type: int or string
// positional arg with no name ":type"
// single named arg with value "name:type"
// single arg with no value "name"
// option group: [arg1 arg2 arg3]
//
// algorithm: iterate args and maintain next expected state

public class ArgReader {
    static class Arg {
        String name;
        String type;

        public Arg(String name, String type) {
            this.name = name;
            this.type = type;
            switch (type) {
            case "int":
            case "string":
            case "var":
            case null:
                break;
            default:
                throw new IllegalStateException("Invalid type in arg spec: " + type);
            }
        }

        boolean hasName() {
            return name != null && !name.isEmpty();
        }

        public boolean hasType() {
            return type != null && !type.isEmpty();
        }

        public boolean isVarArg() {
            return "var".equals(type);
        }
    }

    static class GroupArg extends Arg {
        int group;

        public GroupArg(Arg arg, int group) {
            super(arg.name, arg.type);
            this.group = group;
        }

    }

    String commandName;
    // required args
    List<Arg> requiredArgs = new ArrayList<>();
    // optional arg groups - keyed by unique id (index in the arg spec)
    Map<Integer, Set<GroupArg>> optionGroups = new HashMap<>();

    public ArgReader(String commandName, String[] argSpec) {
        this.commandName = commandName;
        parseArgSpec(argSpec);
    }

    private void parseArgSpec(String[] argSpec) {
        boolean foundVarArg = false;
        for (int i = 0; i < argSpec.length; i++) {
            String s = argSpec[i];
            if (s.startsWith("[")) {
                String[] options = s.substring(1, s.length() - 1).split(" ");
                Set<GroupArg> group = new HashSet<>();
                boolean groupHasVarArg = false;
                for (String option : options) {
                    GroupArg groupArg = parseGroupArg(option, i);
                    if (groupArg.isVarArg()) {
                        groupHasVarArg = true;
                    }
                    group.add(groupArg);
                }
                if (groupHasVarArg && foundVarArg) {
                    throw new IllegalStateException(
                            String.format(
                                    "%s: Invalid arg spec - there can be only one option group with a var arg",
                                    commandName));
                }
                if (groupHasVarArg) {
                    foundVarArg = true;
                }
                optionGroups.put(i, group);
            } else {
                requiredArgs.add(parseArg(s));
            }
        }
    }

    private GroupArg parseGroupArg(String option, int i) {
        return new GroupArg(parseArg(option), i);
    }

    private Arg parseArg(String s) {
        String[] parts = s.split(":");
        if (parts.length == 1) {
            return new Arg(parts[0], null);
        } else {
            return new Arg(parts[0], parts[1]);
        }
    }

    // returned map has unnamed positional args keyed by index
    public Map<String, RespValue> readArgs(RespValue[] args) throws IllegalArgumentException {
        HashMap<String, RespValue> optionMap = new HashMap<>();
        int i = 0;
        for (int j = 0; j < requiredArgs.size(); j++) {
            Arg arg = requiredArgs.get(j);
            if (i >= args.length) {
                throw new IllegalArgumentException(String.format(
                        "%s: Missing required arg '%s' at index %d", commandName, arg.name, i));
            }
            i = readArg(args, commandName, optionMap, i, j, arg);
        }
        Map<Integer, RespValue> foundGroupOptions = new HashMap<>();
        while (i < args.length) {
            RespValue next = args[i];
            // get option and group id
            GroupArg arg = findGroupArg(next);
            if (arg == null) {
                throw new IllegalArgumentException(String
                        .format("%s: unrecognized arg at index %d, %s", commandName, i, next));
            }

            // validate that we have not already seen the option group
            validateArgConflict(args, i, arg.group, foundGroupOptions);

            // it's ok so now add the option value
            i = readArg(args, commandName, optionMap, i, i, arg);

            // succeeded to add it, so store as the group value
            foundGroupOptions.put(arg.group, next);
        }
        return optionMap;
    }

    private GroupArg findGroupArg(RespValue next) {
        String nextArgName = next.getValueAsString().toLowerCase();
        for (Set<GroupArg> group : optionGroups.values()) {
            for (GroupArg groupArg : group) {
                if (groupArg.hasName() && groupArg.name.equals(nextArgName)) {
                    return groupArg;
                }
            }
        }
        return null;
    }

    private int readArg(RespValue[] args, String commandName, HashMap<String, RespValue> optionMap,
            int i, int j, Arg arg) {
        if (!arg.hasName()) {
            validateArgType(args, i, arg);
            if (arg.isVarArg()) {
                List<RespValue> varArgs = new ArrayList<>();
                for (; i < args.length; i++) {
                    varArgs.add(args[i]);
                }
                optionMap.put(String.valueOf(j), RespValue.array(varArgs));
            } else {
                optionMap.put(String.valueOf(j), args[i]);
            }
        } else {
            validateArgEquals(args, i, arg.name);
            if (arg.isVarArg()) {
                i++;
                List<RespValue> varArgs = new ArrayList<>();
                for (; i < args.length; i++) {
                    varArgs.add(args[i]);
                }
                optionMap.put(arg.name, RespValue.array(varArgs));
            } else if (arg.hasType()) {
                i++;
                if (i >= args.length) {
                    throw new IllegalArgumentException(
                            String.format("%s: Missing value for arg '%s' at index %d", commandName,
                                    arg.name, i));
                }
                validateArgType(args, i, arg);
                optionMap.put(arg.name, args[i]);
            } else {
                optionMap.put(arg.name, RespConstants.NULL_VALUE);
            }
        }
        return i + 1;
    }

    private void validateArgType(RespValue[] args, int index, Arg argSpec) {
        if (argSpec.type == null) {
            return;
        }
        RespValue arg = args[index];
        if (argSpec.type.equals("int")) {
            validateArgIsInteger(args, index);
        } else if (argSpec.type.equals("string")) {
            validateArgIsString(args, index);
        } else if (!argSpec.type.equals("var")) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid value for arg '%s' expected type %s at index %d: %s",
                            commandName, argSpec.name, argSpec.type, index, arg));
        }
        return;
    }

    protected void validateArgEquals(RespValue[] args, int index, String expectedValue) {
        RespValue arg = args[index];
        if (!expectedValue.equals(arg.getValueAsString().toLowerCase())) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid arg, expected '%s' at index %d: %s", commandName,
                            expectedValue, index, arg));
        }
    }

    protected void validateArgIsString(RespValue[] args, int index) {
        RespValue arg = args[index];
        if (!arg.isBulkString() && !arg.isSimpleString()) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid arg type, expected string at index %d: %s",
                            commandName, index, arg));
        }
    }

    public void validateArgIsInteger(RespValue[] args, int index) {
        RespValue arg = args[index];
        if (arg.getValueAsLong() == null) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid arg type, expected integer at index %d: %s",
                            commandName, index, arg));
        }
    }

    public void validateArgConflict(RespValue[] args, int index, int optionGroup,
            Map<Integer, RespValue> foundGroupOptions) {
        RespValue arg = args[index];
        if (foundGroupOptions.containsKey(optionGroup)) {
            throw new IllegalArgumentException(
                    String.format("%s: Invalid arg at index %d: %s conflicts with %s", commandName,
                            index, arg, foundGroupOptions.get(optionGroup)));
        }
    }
}
