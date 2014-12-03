package com.latticeengines.domain.exposed.eai;

public enum SourceType {

    MARKETO("Marketo"), //
    ELOQUA("Eloqua"), //
    SALESFORCE("Salesforce");
    
    private String name;
    
    SourceType(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
}
