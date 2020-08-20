package com.latticeengines.domain.exposed.cdl.scheduling.queue;

import java.util.LinkedList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.Constraint;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.HasPAQuota;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargePA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargeTxnPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxScheduleNowPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.RetryNotExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.ScheduleNowExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantDuplicate;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantGroupQuota;

public class ScheduleNowSchedulingPAObject extends SchedulingPAObject {

    /**
     * this list of constraint is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    private static List<Constraint> pushConstraintList;
    /**
     * this list of constraint is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    private static List<Constraint> popConstraintList;

    static {
        initPushConstraint();
        initPopConstraint();
    }

    public ScheduleNowSchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
    }

    @Override
    List<Constraint> getPushConstraints() {
        return pushConstraintList;
    }

    @Override
    List<Constraint> getPopConstraints() {
        return popConstraintList;
    }

    @Override
    public String getConsumedPAQuotaName() {
        return SchedulerConstants.QUOTA_SCHEDULE_NOW;
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare(o.getTenantActivity());
    }

    public int compare(TenantActivity o) {
        return o.getScheduleTime() - this.getTenantActivity().getScheduleTime() > 0 ? -1 : 1;
    }

    private static void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new ScheduleNowExist());
        pushConstraintList.add(new RetryNotExist());
    }

    private static void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxScheduleNowPA());
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxLargeTxnPA());
        popConstraintList.add(new TenantDuplicate());
        popConstraintList.add(new TenantGroupQuota());
        popConstraintList.add(new HasPAQuota(SchedulerConstants.QUOTA_SCHEDULE_NOW, "schedule now"));
    }

}
