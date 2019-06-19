package com.latticeengines.actors.template;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TERMINATE_EXECUTOR_TIMEOUT_MS;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Traveler;
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

    private ExecutorService executor;
    // flag to indicate whether background executors should keep running
    private volatile boolean shouldTerminate = false;

    @Override
    public void postStop() {
        try {
            if (shouldTerminate) {
                return;
            }
            log.info("Shutting down executors");
            shouldTerminate = true;
            if (executor != null) {
                executor.shutdownNow();
                executor.awaitTermination(TERMINATE_EXECUTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            }
            log.info("Completed shutting down of executors");
        } catch (Exception e) {
            log.error("Fail to finish all post-stop actions", e);
        }
    }

    @Override
    protected boolean needAssistantActor() {
        return false;
    }

    /**
     * Override this method if any traveler validation needs to be done.
     * 
     * Throw exception when validation failed.
     * 
     * @param traveler
     */
    protected void validateTraveler(Traveler traveler) {
    }

    @Override
    protected boolean process(Traveler traveler) {
        // Inject failure only for testing purpose
        injectFailure(traveler);
        validateTraveler(traveler);
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

            executor = ThreadPoolUtils.getFixedSizeThreadPool(getActorName(self()) + "-Executor",
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
            while (!shouldTerminate) {
                Traveler traveler = null;
                synchronized (travelers) {
                    while (!shouldTerminate && travelers.isEmpty()) {
                        try {
                            travelers.wait();
                        } catch (InterruptedException e) {
                            if (!shouldTerminate) {
                                log.warn("Executor (in background) is interrupted");
                            }
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
                    traveler.error(
                            String.format("Force to return anchor due to exception encountered at %s: %s",
                                    getActorName(self()), e.getMessage()),
                            e);
                    forceReturnToAnchor(traveler);
                }
            }
        }

    }
}
