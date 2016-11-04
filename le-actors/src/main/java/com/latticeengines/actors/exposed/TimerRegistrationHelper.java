package com.latticeengines.actors.exposed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.actors.ActorTemplate;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.FiniteDuration;

public class TimerRegistrationHelper {
    private static final Log log = LogFactory.getLog(TimerRegistrationHelper.class);

    private Lock lock = new ReentrantLock();
    private volatile boolean registeredTimerFlag = false;
    private Class<? extends ActorTemplate> actorClazz;

    public TimerRegistrationHelper(Class<? extends ActorTemplate> actorClazz) {
        this.actorClazz = actorClazz;
    }

    public void register(ActorSystem system, ActorRef actorRef, int timerFrequency, TimeUnit timeUnit) {
        register(system, actorRef, timerFrequency, timeUnit, null);
    }

    public void register(ActorSystem system, ActorRef actorRef, int timerFrequency, TimeUnit timeUnit,
            TimerMessage timerMessage) {
        TimerMessage timerMessageObj = null;

        if (timerMessage == null) {
            timerMessageObj = new TimerMessage(actorClazz);
        } else {
            timerMessageObj = timerMessage;
        }

        if (!registeredTimerFlag) {
            try {
                lock.lock();
                if (!registeredTimerFlag) {
                    registerTimer(system, actorRef, timerFrequency, timeUnit, timerMessageObj);
                    registeredTimerFlag = true;
                    log.info("Registered for timer call for " + actorClazz.getName());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public void registerTimer(ActorSystem system, ActorRef actorRef, //
            int timerFrequency, TimeUnit timeUnit, TimerMessage timerMessage) {
        system.scheduler().schedule(//
                FiniteDuration.create(0, TimeUnit.MILLISECONDS), //
                FiniteDuration.create(timerFrequency, timeUnit), //
                actorRef, //
                timerMessage, //
                system.dispatcher(), //
                null);
    }
}
