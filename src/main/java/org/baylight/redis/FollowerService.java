package org.baylight.redis;

import java.time.Clock;

public class FollowerService extends RedisServiceBase {

    public FollowerService(RedisServiceOptions options, Clock clock) {
        super(options, clock);
    }

    @Override
    public void getReplcationInfo(StringBuilder sb) {
        // nothing to add for now
    }

}
