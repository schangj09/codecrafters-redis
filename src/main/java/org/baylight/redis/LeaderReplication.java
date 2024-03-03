package org.baylight.redis;

import java.util.HashMap;
import java.util.Map;

public class LeaderReplication {
    String replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    long totalReplicationOffset = 0L;
    Map<String, Long> replicationOffsets = new HashMap<>();

    public LeaderReplication() {

    }

    long getOffset(String follower) {
        return replicationOffsets.getOrDefault(follower, 0L);
    }

    public String getReplicationId() {
        return replicationId;
    }

    public long getTotalReplicationOffset() {
        return totalReplicationOffset;
    }

    public void getReplcationInfo(StringBuilder sb) {
        sb.append("master_replid:").append(replicationId).append("\n");
        sb.append("master_repl_offset:").append(totalReplicationOffset).append("\n");
    }

}
