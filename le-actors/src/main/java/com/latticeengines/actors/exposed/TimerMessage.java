package com.latticeengines.actors.exposed;

public class TimerMessage {
    private Class<?> actorClazz;
    private Object context;

    public TimerMessage(Class<?> actorClazz) {
        this.actorClazz = actorClazz;
    }

    public Class<?> getActorClazz() {
        return actorClazz;
    }

    public void setContext(Object context) {
        this.context = context;
    }

    public Object getContext() {
        return context;
    }
}
