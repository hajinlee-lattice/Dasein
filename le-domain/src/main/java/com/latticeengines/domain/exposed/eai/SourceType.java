package com.latticeengines.domain.exposed.eai;

public enum SourceType {

    MARKETO("Marketo", true), //
    ELOQUA("Eloqua", true), //
    SALESFORCE("Salesforce", true), //
    FILE("File", false), //
    VISIDB("VisiDB", true);

    private String name;
    private boolean willSubmitEaiJob;

    SourceType(String name, boolean willSubmitEaiJob) {
        this.name = name;
        this.willSubmitEaiJob = willSubmitEaiJob;
    }

    public static SourceType getByName(String sourceName) {
        return SourceType.valueOf(sourceName.toUpperCase());
    }

    public String getName() {
        return name;
    }

    public boolean willSubmitEaiJob() {
        return willSubmitEaiJob;
    }

}
