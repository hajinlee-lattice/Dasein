package com.latticeengines.domain.exposed.eai;

public enum SourceType {

    MARKETO("Marketo", true), //
    ELOQUA("Eloqua", true), //
    SALESFORCE("Salesforce", true), //
    FILE("File", false);

    private String name;
    private boolean willSubmitEaiJob;

    SourceType(String name, boolean willSubmitEaiJob) {
        this.name = name;
        this.willSubmitEaiJob = willSubmitEaiJob;
    }

    public String getName() {
        return name;
    }

    public boolean willSubmitEaiJob() {
        return willSubmitEaiJob;
    }

}
