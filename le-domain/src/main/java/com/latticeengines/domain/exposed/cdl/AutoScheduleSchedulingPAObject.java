package com.latticeengines.domain.exposed.cdl;

import java.util.LinkedList;
import java.util.List;

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
    public int compareTo(SchedulingPAObject o) {
        return compare(o.getTenantActivity());
    }

    public int compare(TenantActivity o) {
        return o.getInvokeTime() != null && o.getInvokeTime() - this.getTenantActivity().getInvokeTime() > 0 ? -1 : 1;
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
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new TenantDuplicate());
    }
}
