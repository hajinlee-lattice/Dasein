package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.LinkedList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.constraint.AutoScheduleExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.Constraint;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.FirstActionTimePending;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.LastActionTimePending;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargePA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargeTxnPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.RetryNotExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantDuplicate;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantGroupQuota;

public class AutoScheduleSchedulingPAObject extends SchedulingPAObject {

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
        initPopConstraint();
        initPushConstraint();
    }

    public AutoScheduleSchedulingPAObject(TenantActivity tenantActivity) {
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
        return SchedulerConstants.QUOTA_AUTO_SCHEDULE;
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        return compare(o.getTenantActivity());
    }

    public int compare(TenantActivity o) {
        if (this.getTenantActivity().getInvokeTime() == null || o.getInvokeTime() == null || o.getInvokeTime() - this.getTenantActivity().getInvokeTime() == 0) {
            return o.getFirstActionTime() - this.getTenantActivity().getFirstActionTime() > 0 ? -1 : 1;
        } else {
            return o.getInvokeTime() == null || (this.getTenantActivity().getInvokeTime() != null && o.getInvokeTime() != null && o.getInvokeTime() - this.getTenantActivity().getInvokeTime() > 0) ? -1
                    : 1;
        }
    }

    private static void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new AutoScheduleExist());
        pushConstraintList.add(new FirstActionTimePending());
        pushConstraintList.add(new LastActionTimePending());
        pushConstraintList.add(new RetryNotExist());
    }

    private static void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxLargeTxnPA());
        popConstraintList.add(new TenantDuplicate());
        popConstraintList.add(new TenantGroupQuota());
    }
}
