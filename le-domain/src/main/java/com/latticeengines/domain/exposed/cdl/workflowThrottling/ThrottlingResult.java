package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.Set;

public class ThrottlingResult {
    private Set<String> stillEnqueuedTenantIds; // customerSpace of tenants still enqueued after this cycle
    private Set<String> canSubmitTenantIds; // customerSpace of tenants submitted in this cycle
    private Set<Long> canSubmitWorkflowJobIds;
    private Set<Long> stillEnqueuedWorkflowJobIds;

    public ThrottlingResult(Set<String> stillEnqueuedTenantIds, Set<String> canSubmitTenantIds, Set<Long> canSubmitWorkflowJobIds, Set<Long> stillEnqueuedWorkflowJobIds) {
        this.stillEnqueuedTenantIds = stillEnqueuedTenantIds;
        this.canSubmitTenantIds = canSubmitTenantIds;
        this.canSubmitWorkflowJobIds = canSubmitWorkflowJobIds;
        this.stillEnqueuedWorkflowJobIds = stillEnqueuedWorkflowJobIds;
    }

    public Set<String> getStillEnqueuedTenantIds() {
        return stillEnqueuedTenantIds;
    }

    public void setStillEnqueuedTenantIds(Set<String> stillEnqueuedTenantIds) {
        this.stillEnqueuedTenantIds = stillEnqueuedTenantIds;
    }

    public Set<String> getCanSubmitTenantIds() {
        return canSubmitTenantIds;
    }

    public void setCanSubmitTenantIds(Set<String> canSubmitTenantIds) {
        this.canSubmitTenantIds = canSubmitTenantIds;
    }

    public Set<Long> getCanSubmitWorkflowJobIds() {
        return canSubmitWorkflowJobIds;
    }

    public void setCanSubmitWorkflowJobIds(Set<Long> canSubmitWorkflowJobIds) {
        this.canSubmitWorkflowJobIds = canSubmitWorkflowJobIds;
    }

    public Set<Long> getStillEnqueuedWorkflowJobIds() {
        return stillEnqueuedWorkflowJobIds;
    }

    public void setStillEnqueuedWorkflowJobIds(Set<Long> stillEnqueuedWorkflowJobIds) {
        this.stillEnqueuedWorkflowJobIds = stillEnqueuedWorkflowJobIds;
    }
}
