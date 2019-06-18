package com.latticeengines.domain.exposed.cdl;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class RetrySchedulingPAObject extends SchedulingPAObject {

    /**
     * this list of constraint is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    private List<Constraint> pushConstraintList;
    /**
     * this list of constraint is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    private List<Constraint> popConstraintList;

    public RetrySchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
        initPushContraint();
        initPopContraint();
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare(o.getTenantActivity());
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
        return RetrySchedulingPAObject.class;
    }

    public int compare(TenantActivity o) {
        return o.getLastFinishTime() - this.getTenantActivity().getLastFinishTime() > 0 ? -1 : 1;
    }

    private void initPushContraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new RetryExist());
        pushConstraintList.add(new LastFinishTimePending());
    }

    private void initPopContraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new TenantDuplicate());
    }
}
