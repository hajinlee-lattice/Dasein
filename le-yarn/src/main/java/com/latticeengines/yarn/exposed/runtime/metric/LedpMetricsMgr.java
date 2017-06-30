package com.latticeengines.yarn.exposed.runtime.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

public class LedpMetricsMgr {

    private List<LedpMetrics> ledpMetrics = new ArrayList<LedpMetrics>();
    private long appSubmissionTime;
    private long appStartTime;
    private long containerLaunchTime;
    private long containerEndTime;
    private MetricsSystem ms;

    private static LedpMetricsMgr instance;

    public LedpMetricsMgr() {
        this.ms = DefaultMetricsSystem.instance();
    }

    /**
     * Create as many instances of LedpMetrics as the number of dimensions
     * 
     * @param appAttemptId
     */
    private LedpMetricsMgr(String appAttemptId) {
        this.ms = DefaultMetricsSystem.instance();
        ledpMetrics.add(LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Priority })));
        ledpMetrics.add(LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Queue })));
        ledpMetrics.add(LedpMetrics.getForTags(ms,
                Arrays.<MetricsInfo> asList(new MetricsInfo[] { LedpMetricsInfo.Customer })));
    }

    public static LedpMetricsMgr getInstance(String appAttemptId) {
        if (instance == null) {
            synchronized (LedpMetricsMgr.class) {
                if (instance == null) {
                    instance = new LedpMetricsMgr(appAttemptId);
                }
            }
        }

        return instance;
    }

    public void start() {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.buildTags();
            }
        }.execute();
        ms.init("ledpjob");
    }

    public long getAppStartTime() {
        return appStartTime;
    }

    public void setAppStartTime(long appStartTime) {
        this.appStartTime = appStartTime;
    }

    public long getContainerLaunchTime() {
        return containerLaunchTime;
    }

    public void setContainerLaunchTime(final long containerLaunchTime) {
        this.containerLaunchTime = containerLaunchTime;
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setContainerWaitTime(containerLaunchTime - getAppStartTime());
            }
        }.execute();
    }

    public void setContainerId(String containerId) {
    }

    public void setPriority(final String priority) {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setTagValue(LedpMetricsInfo.Priority, priority);
            }
        }.execute();
    }

    public void incrementNumberPreemptions() {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.incrementNumContainerPreemptions();
            }
        }.execute();

    }

    public void setQueue(final String queue) {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setTagValue(LedpMetricsInfo.Queue, queue);
            }
        }.execute();
    }

    public void setCustomer(final String customer) {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setTagValue(LedpMetricsInfo.Customer, customer);
            }
        }.execute();
    }

    public void setAppEndTime(final long appEndTime) {
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setApplicationCleanupTime(appEndTime - getContainerEndTime());
            }
        }.execute();

    }

    public void setContainerEndTime(final long containerEndTime) {
        this.containerEndTime = containerEndTime;
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setContainerElapsedTime(containerEndTime - getContainerLaunchTime());
            }
        }.execute();
    }

    public long getContainerEndTime() {
        return containerEndTime;
    }

    public long getAppSubmissionTime() {
        return appSubmissionTime;
    }

    public void setAppSubmissionTime(final long appSubmissionTime) {
        this.appSubmissionTime = appSubmissionTime;
        new DoForAllMetrics(ledpMetrics) {
            @Override
            public void execute(LedpMetrics ledpMetric) {
                ledpMetric.setApplicationWaitTime(getAppStartTime() - appSubmissionTime);
            }
        }.execute();

    }
    
    public void publishMetricsNow() {
        ms.publishMetricsNow();
    }

    private abstract class DoForAllMetrics {
        private List<LedpMetrics> ledpMetrics;

        DoForAllMetrics(List<LedpMetrics> ledpMetrics) {
            this.ledpMetrics = ledpMetrics;
        }

        public void execute() {
            for (LedpMetrics lm : ledpMetrics) {
                execute(lm);
            }
        }

        public abstract void execute(LedpMetrics ledpMetric);
    }

}
