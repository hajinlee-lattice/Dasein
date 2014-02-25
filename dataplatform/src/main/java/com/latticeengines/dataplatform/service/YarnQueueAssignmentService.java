package com.latticeengines.dataplatform.service;

import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.NotImplementedException;

public interface YarnQueueAssignmentService {

    public enum AssignmentPolicy {
        DEFAULT,
        // by active + pending
        SHORTESTQUEUE,
        // round robin
        ROUNDROBIN,
        // random
        RANDOM,
        // by pending
        SHORTESTPENDINGQUEUE,
        // by total active + pending resources (memory)
        SHORTESTQUEUEBYRESOURCES,
        // sticky to minimize data movement
        DATALOCALITY;

        public static AssignmentPolicy getEnum(String s) {
            if (DEFAULT.name().equalsIgnoreCase(s)) {
                return DEFAULT;
            } else if (SHORTESTQUEUE.name().equalsIgnoreCase(s)) {
                return SHORTESTQUEUE;
            } else if (ROUNDROBIN.name().equalsIgnoreCase(s)) {
                return ROUNDROBIN;
            } else if (RANDOM.name().equalsIgnoreCase(s)) {
                return RANDOM;
            } else if (SHORTESTPENDINGQUEUE.name().equalsIgnoreCase(s)) {
                return SHORTESTPENDINGQUEUE;
            } else if (SHORTESTQUEUEBYRESOURCES.name().equalsIgnoreCase(s)) {
                return SHORTESTQUEUEBYRESOURCES;
            } else if (DATALOCALITY.name().equalsIgnoreCase(s)) {
                return DATALOCALITY;
            }
            throw new IllegalArgumentException(
                    "No Enum specified for this string");
        }
    }

    String assignToQueue(String assignmentToken, AssignmentPolicy policy)
            throws UnsupportedEncodingException, NotImplementedException;

}
