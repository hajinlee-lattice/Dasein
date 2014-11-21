package com.latticeengines.eai.routes;

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
