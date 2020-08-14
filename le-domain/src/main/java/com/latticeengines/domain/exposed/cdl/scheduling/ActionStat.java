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

    private final Date firstIngestActionTime;

    public ActionStat(Long tenantPid, Date firstActionTime, Date lastActionTime) {
        this(tenantPid, firstActionTime, lastActionTime, null);
    }

    public ActionStat(Long tenantPid, Date firstActionTime, Date lastActionTime, Date firstIngestActionTime) {
        this.tenantPid = tenantPid;
        this.firstActionTime = firstActionTime;
        this.lastActionTime = lastActionTime;
        this.firstIngestActionTime = firstIngestActionTime;
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

    public Date getFirstIngestActionTime() {
        return firstIngestActionTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ActionStat that = (ActionStat) o;
        return Objects.equal(tenantPid, that.tenantPid) && Objects.equal(firstActionTime, that.firstActionTime)
                && Objects.equal(lastActionTime, that.lastActionTime)
                && Objects.equal(firstIngestActionTime, that.firstIngestActionTime);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tenantPid, firstActionTime, lastActionTime, firstIngestActionTime);
    }

    @Override
    public String toString() {
        return "ActionStat{" + "tenantPid=" + tenantPid + ", firstActionTime=" + firstActionTime + ", lastActionTime="
                + lastActionTime + ", firstIngestActionTime=" + firstIngestActionTime + '}';
    }
}
