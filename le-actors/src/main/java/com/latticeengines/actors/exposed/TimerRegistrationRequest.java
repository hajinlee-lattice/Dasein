package com.latticeengines.actors.exposed;

import java.util.concurrent.TimeUnit;

public class TimerRegistrationRequest {
    private int timerFrequency;
    private TimeUnit timeUnit;
    private TimerMessage timerMessage;

    public TimerRegistrationRequest(int timerFrequency, TimeUnit timeUnit, TimerMessage timerMessage) {
        super();
        this.timerFrequency = timerFrequency;
        this.timeUnit = timeUnit;
        this.timerMessage = timerMessage;
    }

    public int getTimerFrequency() {
        return timerFrequency;
    }

    public void setTimerFrequency(int timerFrequency) {
        this.timerFrequency = timerFrequency;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    public TimerMessage getTimerMessage() {
        return timerMessage;
    }

    public void setTimerMessage(TimerMessage timerMessage) {
        this.timerMessage = timerMessage;
    }

}
