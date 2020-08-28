package com.latticeengines.domain.exposed.cdl.scheduling.queue;

import java.util.LinkedList;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.scheduling.SchedulerConstants;
import com.latticeengines.domain.exposed.cdl.scheduling.TenantActivity;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.Constraint;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.DataCloudRefreshExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.HasPAQuota;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargePA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxLargeTxnPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.MaxPA;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.NotHandHoldTenant;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.RetryNotExist;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantDuplicate;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.TenantGroupQuota;

public class DataCloudRefreshSchedulingPAObject extends SchedulingPAObject {

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

    public DataCloudRefreshSchedulingPAObject(TenantActivity tenantActivity) {
        super(tenantActivity);
    }

    private static void initPushConstraint() {
        pushConstraintList = new LinkedList<>();
        pushConstraintList.add(new DataCloudRefreshExist());
        pushConstraintList.add(new RetryNotExist());
        pushConstraintList.add(new NotHandHoldTenant());
    }

    private static void initPopConstraint() {
        popConstraintList = new LinkedList<>();
        popConstraintList.add(new MaxPA());
        popConstraintList.add(new MaxLargePA());
        popConstraintList.add(new MaxLargeTxnPA());
        popConstraintList.add(new TenantDuplicate());
        popConstraintList.add(new TenantGroupQuota());
        popConstraintList.add(new HasPAQuota(SchedulerConstants.QUOTA_AUTO_SCHEDULE, "auto schedule"));
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
        // TODO decide whether datacloud refresh PA should consume auto schedule quota
        // or not
        return SchedulerConstants.QUOTA_AUTO_SCHEDULE;
    }

    @Override
    public int compareTo(SchedulingPAObject o) {
        return super.compareTo(o);
    }
}
