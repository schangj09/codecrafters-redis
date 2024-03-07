package org.baylight.redis;

import java.io.IOException;

public class ConnectionToFollower {
    private final LeaderService service;
    private final ClientConnection followerConnection;
    private volatile boolean handshakeComplete = false;

    public ConnectionToFollower(LeaderService service, ClientConnection followerConnection) throws IOException {
        this.service = service;
        this.followerConnection = followerConnection;
    }

    public boolean isHandshakeComplete() {
        return handshakeComplete;
    }

    public void setHandshakeComplete() {
        this.handshakeComplete = true;
    }

	public ClientConnection getFollowerConnection() {
		return followerConnection;
	}

}
