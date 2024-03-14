package org.baylight.redis.protocol;

public abstract class RespValueBase implements RespValue {
    private final RespType type;
    private RespValueContext context;

    protected RespValueBase(RespType type) {
        this.type = type;
    }

    @Override
    public RespType getType() {
        return type;
    }

    /**
     * @return the context, may be null if not set
     */
    public RespValueContext getContext() {
        return context;
    }

    /**
     * @param context the context to set
     */
    public void setContext(RespValueContext context) {
        this.context = context;
    }

}
