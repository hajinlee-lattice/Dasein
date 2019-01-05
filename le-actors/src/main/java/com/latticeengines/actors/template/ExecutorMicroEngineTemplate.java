package com.latticeengines.actors.template;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;

/**
 * Actors in decision graph have 3 types: anchor, micro-engine & junction
 * 
 * Anchor is entry/exit actor
 * 
 * Micro-engine is actors where traveler travels around. Micro-engine has 2
 * types: proxy micro-engine & executor micro-engine. Proxy micro-engine needs
 * assistant actor to finish task. Executor micro-engine does the task by itself
 * asynchronously.
 * 
 * Junction is the connecting point between decision graph/actor system
 */
public abstract class ExecutorMicroEngineTemplate extends VisitorActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(ExecutorMicroEngineTemplate.class);

    /**
     * Get thread pool size of executors
     * 
     * @return
     */
    protected abstract int getExecutorNum();

    /**
     * Implement detailed task execution based on traveler
     * 
     * @param traveler
     */
    protected abstract void execute(Traveler traveler);

    // Whether executors are initialized
    private final AtomicBoolean executorInitiated = new AtomicBoolean(false);
    // Travelers waiting to be processed
    private final Queue<Traveler> travelers = new ConcurrentLinkedQueue<>();

    @Override
    protected boolean needAssistantActor() {
        return false;
    }

    @Override
    protected boolean process(Traveler traveler) {
        getGuideBook().logVisit(ActorUtils.getPath(self()), traveler);
        if (accept(traveler)) {
            if (!executorInitiated.get()) {
                initExecutors();
            }
            travelers.offer(traveler);
            synchronized (travelers) {
                if (travelers.size() > 0) {
                    travelers.notifyAll();
                }
            }
            return true;
        } else {
            traveler.debug("Rejected by " + getActorSystem().getActorName(self()));
            return false;
        }
    }

    private void initExecutors() {
        synchronized (executorInitiated) {
            if (executorInitiated.get()) {
                // do nothing if executors are already started
                return;
            }

            ExecutorService executor = ThreadPoolUtils.getFixedSizeThreadPool(getActorName(self()) + "-Executor",
                    getExecutorNum());
            IntStream.range(0, getExecutorNum()).forEach(i -> {
                executor.submit(new Executor());
            });

            executorInitiated.set(true);
            log.info("Executors at " + getActorName(self()) + " are initialized.");
        }
    }

    /**
     * Executor only picks one traveler to process at one time. Usually
     * ExecutorMicroEngine works on computation oriented tasks. Don't see
     * batching use case for now. If batching is needed in the future, could add
     * it.
     */
    private class Executor implements Runnable {

        @Override
        public void run() {
            while (true) {
                Traveler traveler = null;
                synchronized (travelers) {
                    while (travelers.isEmpty()) {
                        try {
                            travelers.wait();
                        } catch (InterruptedException e) {
                            log.error(String.format("Encounter InterruptedException in executor at %s: %s",
                                    getActorName(self()), e.getMessage()));
                        }
                    }
                    traveler = travelers.poll();
                }
                if (traveler == null) {
                    continue;
                }
                try {
                    execute(traveler);
                    travel(traveler, self(), false);
                } catch (Exception e) {
                    traveler.warn(
                            String.format("Force to return anchor due to exception encountered at %s: %s",
                                    getActorName(self()), e.getMessage()),
                            e);
                    forceReturnToAnchor(traveler);
                }
            }
        }

    }
}
