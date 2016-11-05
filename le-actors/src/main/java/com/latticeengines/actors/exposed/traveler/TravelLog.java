package com.latticeengines.actors.exposed.traveler;

import org.apache.log4j.Level;

public class TravelLog {

    private final String message;
    private final Level level;
    private final Throwable throwable;

    public TravelLog(Level level, Throwable throwable, String message) {
        this.level = level;
        this.throwable = throwable;
        this.message = message;
    }

    public TravelLog(Level level, String message) {
        this(level, null, message);
    }

    public String getMessage() {
        return message;
    }

    public Level getLevel() {
        return level;
    }

    public Throwable getThrowable() {
        return throwable;
    }

}
