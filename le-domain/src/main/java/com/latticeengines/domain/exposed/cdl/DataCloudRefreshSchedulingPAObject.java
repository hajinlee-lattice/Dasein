package com.latticeengines.domain.exposed.cdl;

import java.util.LinkedList;
import java.util.List;

public class DataCloudRefreshSchedulingPAObject extends SchedulingPAObject {

    /**
     * this list of constraint is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    private List<Constraint> pushConstraintList;
    /**
     * this list of constraint is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    private List<Constraint> popConstraintList;

    public DataCloudRefreshSchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
        initPopConstraint();
        initPushConstraint();
    }

    private void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new DataCloudRefreshExist());
        pushConstraintList.add(new RetryNotExist());
    }

    private void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new TenantDuplicate());
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
    public Class<DataCloudRefreshSchedulingPAObject> getInstance() {
        return DataCloudRefreshSchedulingPAObject.class;
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        return super.compareTo(o);
    }
}
