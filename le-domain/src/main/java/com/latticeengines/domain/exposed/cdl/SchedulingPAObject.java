package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.security.TenantType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RetrySchedulingPAObject.class, name = "RetrySchedulingPAObject"),
        @JsonSubTypes.Type(value = ScheduleNowSchedulingPAObject.class, name = "CustomerPriorityObject"),
        @JsonSubTypes.Type(value = AutoScheduleSchedulingPAObject.class, name = "AutoScheduleSchedulingPAObject"),
        @JsonSubTypes.Type(value = DataCloudRefreshSchedulingPAObject.class, name = "DataCloudRefreshSchedulingPAObject"),
})
public abstract class SchedulingPAObject<T extends SchedulingPAObject> implements Comparable<SchedulingPAObject> {

    /**
     * tenantActivity contains all information we used to sort or check Constraint
     */
    private TenantActivity tenantActivity;

    /**
     * this method is used when schedulingPAObject push into queue. check if this object can push into
     * queue or not.
     */
    abstract boolean checkAddConstraint(SystemStatus systemStatus);
    /**
     * this method is used when schedulingPAObject pop from queue. check if this object can pop queue or not.
     */
    abstract boolean checkPopConstraint(SystemStatus systemStatus, Set<String> scheduledTenants);

    public abstract Class<T> getInstance();

    public SchedulingPAObject(TenantActivity tenantActivity) {
        this.tenantActivity = tenantActivity;
    }

    public int compareTo(SchedulingPAObject o) {
        return compare(o.getTenantActivity());
    }

    public TenantActivity getTenantActivity() {
        return tenantActivity;
    }

    public void setTenantActivity(TenantActivity tenantActivity) {
        this.tenantActivity = tenantActivity;
    }

    private int compare(TenantActivity o) {
        if (o.getTenantType() == tenantActivity.getTenantType()) {
            return 0;
        }
        return o.getTenantType() == TenantType.CUSTOMER ? 1 : -1;
    }

    public boolean checkConstraint(SystemStatus systemStatus, Set<String> scheduledTenants, List<Constraint> constraintList) {
        boolean violated = false;
        for (Constraint constraint : constraintList) {
            if (constraint.checkViolated(systemStatus, scheduledTenants, this.getTenantActivity())) {
                violated = true;
                break;
            }
        }
        return !violated;
    }
}
