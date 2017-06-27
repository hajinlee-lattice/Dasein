package com.latticeengines.domain.exposed.oauth;

public enum OauthClientType {
    LP("lp"), PLAYMAKER("playmaker");

    private final String value;

    OauthClientType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
