package com.turbospaces.poc;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "qualifier")
public abstract class Messages {
    private static final AtomicLong INC = new AtomicLong();

    public MessageHeaders headers;

    public static class UserCommand extends Messages {
        public String username;
        public boolean processed;

        @Override
        public String toString() {
            return Objects.toStringHelper( this ).add( "username", username ).add( "headers", headers ).toString();
        }

        public static UserCommand some(long iteration) {
            UserCommand c = new UserCommand();

            c.username = "user-xxx-" + iteration;

            c.headers = new MessageHeaders();
            c.headers.correlationId = "correlationId" + INC.incrementAndGet();
            c.headers.timestamp = System.currentTimeMillis();
            c.headers.errorCode = (int) iteration;

            return c;
        }
    }

    public static class MessageHeaders {
        public String correlationId;
        public String sessionId;
        public long timestamp;
        public int retry;
        public int errorCode;
    }
}
