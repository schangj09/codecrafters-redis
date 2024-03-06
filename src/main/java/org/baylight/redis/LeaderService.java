package org.baylight.redis;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespSimpleStringValue;
import org.baylight.redis.protocol.RespValue;

public class LeaderService extends RedisServiceBase {
    String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    long totalReplicationOffset = 0L;
    Map<String, Long> replicationOffsets = new HashMap<>();

    public LeaderService(RedisServiceOptions options, Clock clock) {
        super(options, clock);
    }

    long getFollowerOffset(String follower) {
        return replicationOffsets.getOrDefault(follower, 0L);
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getTotalReplicationOffset() {
        return totalReplicationOffset;
    }

    @Override
    public void getReplcationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }

    @Override
    public byte[] replicationConfirm(Map<String, RespValue> optionsMap) {
        return RespConstants.OK;
    }

    @Override
	public byte[] psync(Map<String, RespValue> optionsMap) {
        String response = String.format("FULLRESYNC %s 0", replicationId);
		return new RespSimpleStringValue(response).asResponse();
	}
}
