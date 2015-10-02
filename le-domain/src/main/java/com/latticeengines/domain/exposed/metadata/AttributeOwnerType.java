package com.latticeengines.domain.exposed.metadata;

public enum AttributeOwnerType {

    TABLE((byte) 0),
    PRIMARYKEY((byte) 1),
    LASTMODIFIEDKEY((byte) 2);
    
    byte value;
    
    AttributeOwnerType(byte value) {
        this.value = value;
    }
    
    public byte getValue() {
        return value;
    }
}
