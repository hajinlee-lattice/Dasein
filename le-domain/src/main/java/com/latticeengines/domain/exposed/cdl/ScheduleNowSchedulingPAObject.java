package com.latticeengines.domain.exposed.cdl;

import java.util.LinkedList;
import java.util.List;

public class ScheduleNowSchedulingPAObject extends SchedulingPAObject {

    /**
     * this list of constraint is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    private List<Constraint> pushConstraintList;
    /**
     * this list of constraint is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    private List<Constraint> popConstraintList;

    public ScheduleNowSchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
        initPushConstraint();
        initPopConstraint();
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
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare(o.getTenantActivity());
    }

    @Override
    public Class<ScheduleNowSchedulingPAObject> getInstance() {
        return ScheduleNowSchedulingPAObject.class;
    }

    public int compare(TenantActivity o) {
        return o.getScheduleTime() - this.getTenantActivity().getScheduleTime() > 0 ? -1 : 1;
    }

    private void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new ScheduleNowExist());
        pushConstraintList.add(new RetryNotExist());
    }

    private void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxScheduleNowPA());
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new TenantDuplicate());
    }

}
