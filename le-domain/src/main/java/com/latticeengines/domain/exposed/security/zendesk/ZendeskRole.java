package com.latticeengines.domain.exposed.security.zendesk;

public enum ZendeskRole {
    END_USER("end-user"),
    AGENT("agent"),
    ADMIN("admin");

    private final String name;

    ZendeskRole(String name) {
        this.name = name;
    }

    public static ZendeskRole fromString(String str) {
        for (ZendeskRole role : ZendeskRole.values()) {
            if (role.name.equalsIgnoreCase(str)) {
                return role;
            }
        }
        throw new IllegalArgumentException(String.format("ZendeskRole %s does not exist", str));
    }

    @Override
    public String toString() {
        return name;
    }
}
