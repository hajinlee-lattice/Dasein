package com.latticeengines.domain.exposed.propdata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    private static final Log log = LogFactory.getLog(MatchCommandStatus.class);
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
