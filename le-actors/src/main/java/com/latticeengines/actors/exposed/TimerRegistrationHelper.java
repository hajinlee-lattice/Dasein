package com.latticeengines.actors.exposed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.ActorTemplate;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import scala.concurrent.duration.FiniteDuration;

public class TimerRegistrationHelper {
    private static final Logger log = LoggerFactory.getLogger(TimerRegistrationHelper.class);

    private Lock lock = new ReentrantLock();
    private volatile boolean registeredTimerFlag = false;
    private Class<? extends ActorTemplate> actorClazz;

    public TimerRegistrationHelper(Class<? extends ActorTemplate> actorClazz) {
        this.actorClazz = actorClazz;
    }

    public void register(ActorSystem system, ActorRef actorRef, TimerRegistrationRequest request) {
        List<TimerRegistrationRequest> requests = new ArrayList<>();
        requests.add(request);
        register(system, actorRef, requests);
    }

    public void register(ActorSystem system, ActorRef actorRef,
            List<TimerRegistrationRequest> timerRegistrationRequests) {
        if (!registeredTimerFlag) {
            try {
                lock.lock();
                if (!registeredTimerFlag) {

                    int counter = 0;
                    for (TimerRegistrationRequest timerRegistrationRequest : timerRegistrationRequests) {

                        TimerMessage timerMessageObj = null;

                        if (timerRegistrationRequest.getTimerMessage() == null) {
                            timerMessageObj = new TimerMessage(actorClazz);
                        } else {
                            timerMessageObj = timerRegistrationRequest.getTimerMessage();
                        }

                        registerTimer(system, actorRef, timerRegistrationRequest.getTimerFrequency(),
                                timerRegistrationRequest.getTimeUnit(), timerMessageObj);
                        log.info("Registered timer call " + counter++ + " for " + actorClazz.getName());
                    }
                    registeredTimerFlag = true;
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
