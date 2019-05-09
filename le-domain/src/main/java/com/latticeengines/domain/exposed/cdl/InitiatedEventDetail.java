package com.latticeengines.domain.exposed.cdl;

public class InitiatedEventDetail extends EventDetail {

    public InitiatedEventDetail() {
        super("Initiated");
    }

    private Long batchId;

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }
}
