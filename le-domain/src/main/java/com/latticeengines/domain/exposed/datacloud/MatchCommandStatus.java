package com.latticeengines.domain.exposed.datacloud;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum MatchCommandStatus {

    UNKNOWN("UNKNOWN"),
    NOT_FOUND("NOT FOUND"),
    NEW("NOT STARTED"),
    MATCHING("MATCHING"),
    MATCHED("MATCHED"),
    PROCESSED("PROCESSED"),
    COMPLETING("COMPLETING"),
    COMPLETE("COMPLETE"),
    ABORTED("ABORTED"),
    FAILED("FAILED");

    private static final Logger log = LoggerFactory.getLogger(MatchCommandStatus.class);
    private final String status;
    private static Map<String, MatchCommandStatus> statusMap;

    static {
        statusMap = new ConcurrentHashMap<>();
        for (MatchCommandStatus status: MatchCommandStatus.values()) {
            statusMap.put(status.getStatus(), status);
        }
    }

    MatchCommandStatus(String status) { this.status = status.toUpperCase(); }

    public String getStatus() { return this.status; }

    public static MatchCommandStatus fromStatus(String status) {
        if (statusMap.containsKey(status.toUpperCase())) {
            return statusMap.get(status.toUpperCase());
        } else {
            log.warn("Unknown PropData match status " + status);
            return MatchCommandStatus.UNKNOWN;
        }
    }
}
