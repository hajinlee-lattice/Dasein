package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Date;

/**
 * Contains stats related to {@link com.latticeengines.domain.exposed.pls.Action} that is
 * required by PA scheduler.
 */
public class ActionStat {

    private final Long tenantPid;

    private final Date firstActionTime;

    private final Date lastActionTime;

    public ActionStat(Long tenantPid, Date firstActionTime, Date lastActionTime) {
        this.tenantPid = tenantPid;
        this.firstActionTime = firstActionTime;
        this.lastActionTime = lastActionTime;
    }

    public Long getTenantPid() {
        return tenantPid;
    }

    public Date getFirstActionTime() {
        return firstActionTime;
    }

    public Date getLastActionTime() {
        return lastActionTime;
    }

    @Override
    public String toString() {
        return "ActionStat{" + "tenantPid=" + tenantPid + ", firstActionTime=" + firstActionTime + ", lastActionTime="
                + lastActionTime + '}';
    }
}
