package com.latticeengines.domain.exposed.source;

public enum SourceCredentialType {

    PRODUCTION("Production", true), //

    SANDBOX("Sandbox", false);

    private String name;
    private boolean isProduction;

    SourceCredentialType(String name, boolean isProduction) {
        this.name = name;
        this.isProduction = isProduction;
    }

    public String getName() {
        return name;
    }

    public boolean isProduction() {
        return this.isProduction;
    }
}
