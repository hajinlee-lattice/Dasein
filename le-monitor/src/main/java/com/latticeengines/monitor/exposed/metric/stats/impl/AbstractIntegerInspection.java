package com.latticeengines.monitor.exposed.metric.stats.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.monitor.exposed.metric.stats.Inspection;
import com.latticeengines.monitor.metric.measurement.AtomicIntegerMeas;

public abstract class AbstractIntegerInspection implements Inspection {

    private static final Long RESET_INTERVAL = 10000L;
    private static final Long UPDATE_INTERVAL = 20L;

    String inspectionName;
    private ThreadPoolTaskScheduler scheduler;

    private final AtomicInteger maxSize = new AtomicInteger();
    private final AtomicInteger minSize = new AtomicInteger();
    private final AtomicInteger avgSize = new AtomicInteger();
    private final AtomicInteger counter = new AtomicInteger();

    AbstractIntegerInspection() {}

    protected void bootstrap() {
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                reset();
            }
        }, RESET_INTERVAL);
        scheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                update();
            }
        }, UPDATE_INTERVAL);
    }

    protected void setScheduler(ThreadPoolTaskScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setInspectionName(String inspectionName) {
        this.inspectionName = inspectionName;
    }

    private void update() {
        Integer currentVal = getCurrentValue();
        maxSize.set(Math.max(maxSize.get(), currentVal));
        minSize.set(Math.min(minSize.get(), currentVal));
        int count = counter.incrementAndGet();
        avgSize.set((avgSize.get() * (count - 1) + currentVal) / count);
    }

    private void reset() {
        Integer currentVal = getCurrentValue();
        maxSize.set(currentVal);
        minSize.set(currentVal);
        avgSize.set(currentVal);
        counter.set(1);
    }

    protected abstract Integer getCurrentValue();

    @Override
    public List<Measurement<?, ?>> report() {
        List<Measurement<?, ?>> toReturn = new ArrayList<>();
        toReturn.add(new AtomicIntegerMeas(inspectionName, maxSize, minSize, avgSize));
        return toReturn;
    }

    @Override
    public Long interval() {
        return RESET_INTERVAL;
    }

}
