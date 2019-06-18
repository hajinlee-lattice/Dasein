package com.latticeengines.domain.exposed.cdl;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AutoScheduleSchedulingPAObject extends SchedulingPAObject {

    /**
     * this list of constraint is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    private List<Constraint> pushConstraintList;
    /**
     * this list of constraint is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    private List<Constraint> popConstraintList;

    public AutoScheduleSchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
        initPopConstraint();
        initPushConstraint();
    }

    @Override
    boolean checkAddConstraint(SystemStatus systemStatus) {
        return checkConstraint(systemStatus, null, pushConstraintList);
    }

    @Override
    boolean checkPopConstraint(SystemStatus systemStatus, Set scheduledTenants) {
        return checkConstraint(systemStatus, scheduledTenants, popConstraintList);
    }

    @Override
    public Class getInstance() {
        return AutoScheduleSchedulingPAObject.class;
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        return compare(o.getTenantActivity());
    }

    public int compare(TenantActivity o) {
        return o.getInvokeTime() != null && o.getInvokeTime() - this.getTenantActivity().getInvokeTime() > 0 ? -1 : 1;
    }

    private void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new FirstActionTimePending());
        pushConstraintList.add(new LastActionTimePending());
    }

    private void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new TenantDuplicate());
    }
}
