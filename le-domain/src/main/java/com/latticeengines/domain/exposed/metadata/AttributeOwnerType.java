package com.latticeengines.domain.exposed.metadata;

public enum AttributeOwnerType {

    PRIMARYKEY((byte) 0),
    LASTMODIFIEDKEY((byte) 1);
    
    byte value;
    
    AttributeOwnerType(byte value) {
        this.value = value;
    }
    
    public byte getValue() {
        return value;
    }
}
