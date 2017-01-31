package com.latticeengines.domain.exposed.camille.locks;

import java.util.ArrayList;
import java.util.List;

public class RateLimitedAcquisition {

    private final boolean allowed;
    private long acquiredTimestamp;
    private List<String> rejectionReasons;
    private List<String> exceedingQuotas;

    private RateLimitedAcquisition(boolean allowed) {
        this.allowed = allowed;
        if (!allowed) {
            rejectionReasons = new ArrayList<>();
        }
    }

    public static RateLimitedAcquisition allowed(long acquiredTimestamp) {
        RateLimitedAcquisition acquisition = new RateLimitedAcquisition(true);
        acquisition.setAcquiredTimestamp(acquiredTimestamp);
        return acquisition;
    }

    public static RateLimitedAcquisition disallowed() {
        return new RateLimitedAcquisition(false);
    }

    public boolean isAllowed() {
        return allowed;
    }

    public long getAcquiredTimestamp() {
        return acquiredTimestamp;
    }

    private void setAcquiredTimestamp(long acquiredTimestamp) {
        this.acquiredTimestamp = acquiredTimestamp;
    }

    public List<String> getRejectionReasons() {
        return rejectionReasons;
    }

    public List<String> getExceedingQuotas() {
        return exceedingQuotas;
    }

    public void setExceedingQuotas(List<String> exceedingQuotas) {
        this.exceedingQuotas = exceedingQuotas;
    }

    public RateLimitedAcquisition addRejectionReason(String rejectionReason) {
        if (rejectionReasons == null) {
            rejectionReasons = new ArrayList<>();
        }
        rejectionReasons.add(rejectionReason);
        return this;
    }

    public RateLimitedAcquisition addExceedingQuota(String exceedingQuota) {
        if (exceedingQuotas == null) {
            exceedingQuotas = new ArrayList<>();
        }
        exceedingQuotas.add(exceedingQuota);
        return this;
    }

}
