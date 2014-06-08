package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

public enum ModelCommandStatus {
    NEW(0), IN_PROGRESS(1), SUCCESS(3), FAIL(4);
    
    private int value;
    
    private ModelCommandStatus(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    }
}
