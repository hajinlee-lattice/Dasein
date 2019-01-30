package com.latticeengines.domain.exposed.cdl;

public enum ExternalIntegrationMessageAttribute {

    // keys
    TARGET_SYSTEMS("targetSystems"), //
    TENANT_ID("tenantId"), //
    OPERATION("operation"), //
    ENTITY_NAME("entityName"), //
    ENTITY_ID("entityId"), //
    EXTERNAL_SYSTEM_ID("externalSystemId"), //
    SOURCE_FILE("sourceFile");

    private String name;

    ExternalIntegrationMessageAttribute(String name) {
        this.setName(name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
