package org.baylight.redis.protocol;

public abstract class RespValueBase implements RespValue {
    private final RespType type;

    protected RespValueBase(RespType type) {
        this.type = type;
    }

    @Override
    public RespType getType() {
        return type;
    }

}
