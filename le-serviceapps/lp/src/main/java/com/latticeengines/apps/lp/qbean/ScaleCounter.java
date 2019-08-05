package com.latticeengines.apps.lp.qbean;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;

class ScaleCounter {

    private static final Logger log = LoggerFactory.getLogger(ScaleCounter.class);

    private final String emrCluster;
    private final PriorityQueue<Pair<Integer, Integer>> scaleInPq;
    private final PriorityQueue<Pair<Integer, Integer>> scaleOutPq;
    private final int maxScaleInAttempts;
    private final int maxScaleOutAttempts;
    private long latestScaleOutTime;

    ScaleCounter(String emrCluster, //
                 PriorityQueue<Pair<Integer, Integer>> scaleInPq, PriorityQueue<Pair<Integer, Integer>> scaleOutPq, //
                 int maxScaleInAttempts, int maxScaleOutAttempts, long latestScaleOutTime) {
        this.emrCluster = emrCluster;
        this.scaleInPq = scaleInPq;
        this.scaleOutPq = scaleOutPq;
        this.maxScaleInAttempts = maxScaleInAttempts;
        this.maxScaleOutAttempts = maxScaleOutAttempts;
        this.latestScaleOutTime = latestScaleOutTime;
    }

    long getLatestScaleOutTime() {
        return latestScaleOutTime;
    }

    void setLatestScaleOutTime(long latestScaleOutTime) {
        this.latestScaleOutTime = latestScaleOutTime;
    }

    Pair<Integer, Integer> incrementScaleOutCounter(int target) {
        Pair<Integer, Integer> pair = incrementScaleCounter(-target, true);
        return Pair.of(-pair.getLeft(), pair.getRight());
    }

    Pair<Integer, Integer> incrementScaleInCounter(int target) {
        return incrementScaleCounter(target, false);
    }

    // pair = (target, attempts)
    private Pair<Integer, Integer> incrementScaleCounter(int target, boolean scaleOut) {
        PriorityQueue<Pair<Integer, Integer>> pq = scaleOut ? scaleOutPq: scaleInPq;
        insertTargetToPq(pq, target, scaleOut);
        int maxAttempts = scaleOut ? maxScaleOutAttempts : maxScaleInAttempts;
        return findBestAttempt(pq, maxAttempts, scaleOut);
    }

    private void insertTargetToPq(PriorityQueue<Pair<Integer, Integer>> pq, int target, boolean scaleOut) {
        // consider scaling in:
        // if target not in PQ, its attempt is the max attempts of all targets below it, + 1
        // for all targets in PQ, if the recorded target above the passed in target, attempt + 1
        // if the recorded target is below, discard that counter (because the streak discontinued)
        int maxAttemptsAboveTarget = 0;
        // remove all pairs having more target
        while (!pq.isEmpty()) {
            Pair<Integer, Integer> head = pq.poll();
            if (head.getKey() <= target) {
                // notice that we also removed the record that key == target
                if (scaleOut) {
                    log.info("Discard {} scale out counter: target={}, attempts={}", //
                            emrCluster, -head.getLeft(), head.getRight());
                } else {
                    log.info("Discard {} scale in counter: target={}, attempts={}", //
                            emrCluster, head.getLeft(), head.getRight());
                }
                maxAttemptsAboveTarget = Math.max(maxAttemptsAboveTarget, head.getRight());
            } else {
                pq.offer(head);
                break;
            }
        }
        // for pairs in pq, increment attempts
        List<Pair<Integer, Integer>> collector = new ArrayList<>();
        while(!pq.isEmpty()) {
            Pair<Integer, Integer> pair = pq.poll();
            int oldTarget = pair.getLeft();
            int oldAttempts = pair.getRight();
            int attempts = oldAttempts + 1;
            collector.add(Pair.of(oldTarget, attempts));
            if (scaleOut) {
                log.info("Increment {} scale out counter: target={}, attempts={}", emrCluster, -oldTarget, attempts);
            } else {
                log.info("Increment {} scale in counter: target={}, attempts={}", emrCluster, oldTarget, attempts);
            }
        }
        // add the new value to pq.
        int attempts = maxAttemptsAboveTarget + 1;
        if (scaleOut) {
            log.info("Add {} scale out counter: target={}, attempts={}", emrCluster, -target, attempts);
        } else {
            log.info("Add {} scale in counter: target={}, attempts={}", emrCluster, target, attempts);
        }
        collector.add(Pair.of(target, attempts));
        collector.forEach(pq::offer);
    }

    private Pair<Integer, Integer> findBestAttempt(PriorityQueue<Pair<Integer, Integer>> pq, //
                                                   int maxAttempts, boolean scaleOut) {
        // 1. find the smallest target that reaches max attempts
        // 2. if not found, return the max attempts for all targets
        Pair<Integer, Integer> finalAttempt = Pair.of(0 ,0);
        List<Pair<Integer, Integer>> collector = new ArrayList<>();
        while (!pq.isEmpty()) {
            Pair<Integer, Integer> head = pq.poll();
            collector.add(head);
            if (head.getRight() >= maxAttempts) {
                if (scaleOut) {
                    log.info("Attempts in {} to scale out to {} has reached maximum.", emrCluster, -head.getLeft());
                } else {
                    log.info("Attempts in {} to scale in to {} has reached maximum.", emrCluster, head.getLeft());
                }
                finalAttempt = head;
                break;
            } else if (head.getRight() > finalAttempt.getRight()) {
                finalAttempt = head;
            }
        }
        collector.forEach(pq::offer);
        return finalAttempt;
    }

    void resetScaleInCounter() {
        if (!scaleInPq.isEmpty()) {
            log.info("Reset " + emrCluster + " scale in counter: " + JsonUtils.serialize(scaleInPq));
            scaleInPq.clear();
        }
    }

    void resetScaleOutCounter() {
        if (!scaleOutPq.isEmpty()) {
            log.info("Reset " + emrCluster + " scale out counter: " + JsonUtils.serialize(scaleOutPq));
            scaleOutPq.clear();
        }
    }

    void clearScaleInCounter(int scaleInTarget) {
        clearScaleCounter(scaleInTarget, false);
    }

    void clearScaleOutCounter(int scaleOutTarget) {
        clearScaleCounter(-scaleOutTarget, true);
    }

    private void clearScaleCounter(int scaleTarget, boolean scaleOut) {
        PriorityQueue<Pair<Integer, Integer>> pq = scaleOut ? scaleOutPq : scaleInPq;
        if (CollectionUtils.isNotEmpty(pq)) {
            // wipe out all pairs having higher target
            List<Pair<Integer, Integer>> collector = new ArrayList<>();
            while (!pq.isEmpty()) {
                Pair<Integer, Integer> head = pq.poll();
                if (head.getKey() >= scaleTarget) {
                    if (scaleOut) {
                        log.info("Discard {} scaling out count: target={}, attempts={}", //
                                emrCluster, -head.getLeft(), head.getRight());
                    } else {
                        log.info("Discard {} scaling in count: target={}, attempts={}", //
                                emrCluster, head.getLeft(), head.getRight());
                    }
                } else {
                    collector.add(head);
                }
            }
            collector.forEach(pq::offer);
        }
    }

}
