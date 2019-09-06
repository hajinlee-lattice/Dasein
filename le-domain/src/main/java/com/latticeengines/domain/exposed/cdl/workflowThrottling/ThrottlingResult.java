package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class ThrottlingResult {
    // customerSpace -> workflowJobPids
    private Map<String, List<Long>> stillEnqueued;
    private Map<String, List<Long>> canSubmit;

    public ThrottlingResult(@NotNull Map<String, List<Long>> stillEnqueued, @NotNull Map<String, List<Long>> canSubmit) {
        this.stillEnqueued = stillEnqueued;
        this.canSubmit = canSubmit;
    }

    public Map<String, List<Long>> getStillEnqueued() {
        return stillEnqueued;
    }

    public Map<String, List<Long>> getCanSubmit() {
        return canSubmit;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
