package com.latticeengines.domain.exposed.pls.cdl.channel;

public enum AudienceType {

    ACCOUNTS("Accounts"), CONTACTS("Contacts");

    private String type;

    AudienceType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
