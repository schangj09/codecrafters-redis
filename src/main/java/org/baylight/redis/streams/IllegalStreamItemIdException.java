package org.baylight.redis.streams;

public class IllegalStreamItemIdException extends Exception {
    public IllegalStreamItemIdException(String message) {
        super(message);
    }
}
