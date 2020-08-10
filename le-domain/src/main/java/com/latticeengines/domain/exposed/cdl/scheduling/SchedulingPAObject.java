package com.latticeengines.domain.exposed.cdl.scheduling;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.cdl.scheduling.constraint.Constraint;
import com.latticeengines.domain.exposed.security.TenantType;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = RetrySchedulingPAObject.class, name = "RetrySchedulingPAObject"),
        @JsonSubTypes.Type(value = ScheduleNowSchedulingPAObject.class, name = "CustomerPriorityObject"),
        @JsonSubTypes.Type(value = AutoScheduleSchedulingPAObject.class, name = "AutoScheduleSchedulingPAObject"),
        @JsonSubTypes.Type(value = DataCloudRefreshSchedulingPAObject.class, name = "DataCloudRefreshSchedulingPAObject"),
})
public abstract class SchedulingPAObject implements Comparable<SchedulingPAObject> {

    /**
     * tenantActivity contains all information we used to sort or check Constraint
     */
    private TenantActivity tenantActivity;

    abstract List<Constraint> getPushConstraints();

    abstract List<Constraint> getPopConstraints();

    /*
     * quota name that will be consumed for tenants scheduled through this queue
     */
    public String getConsumedPAQuotaName() {
        return null;
    }

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
            if (o.isLargeTransaction() == tenantActivity.isLargeTransaction()) {
                return 0;
            }
            return o.isLargeTransaction()? 1 : -1;
        }
        return o.getTenantType() == TenantType.CUSTOMER ? 1 : -1;
    }

}
