package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.Date;

import com.google.common.base.Objects;

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
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ActionStat that = (ActionStat) o;
        return Objects.equal(tenantPid, that.tenantPid) && Objects.equal(firstActionTime, that.firstActionTime)
                && Objects.equal(lastActionTime, that.lastActionTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tenantPid, firstActionTime, lastActionTime);
    }

    @Override
    public String toString() {
        return "ActionStat{" + "tenantPid=" + tenantPid + ", firstActionTime=" + firstActionTime + ", lastActionTime="
                + lastActionTime + '}';
    }
}
