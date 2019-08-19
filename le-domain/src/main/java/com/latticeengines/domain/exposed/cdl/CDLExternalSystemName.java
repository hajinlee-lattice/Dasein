package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

public enum CDLExternalSystemName {
    Marketo("Marketo"), //
    Eloqua("Eloqua"), //
    Salesforce("Salesforce"), //
    Facebook("Facebook"), //
    LinkedIn("LinkedIn"), //
    GoogleAds("Google Ads"), //
    AWS_S3("AWS S3"), //
    Outreach("Outreach"), //
    Others("Others");

    private static Map<String, CDLExternalSystemName> map = new HashMap<>();

    static {
        for (CDLExternalSystemName sysName : CDLExternalSystemName.values()) {
            map.put(sysName.name().toUpperCase(), sysName);
            map.put(sysName.getDisplayName().toUpperCase(), sysName);
        }
    }

    private String displayName;

    CDLExternalSystemName(String displayName) {
        this.displayName = displayName;
    }

    public static CDLExternalSystemName getSystemNameEnum(String systemName) {
        if (systemName == null) {
            return null;
        }
        return map.get(systemName.toUpperCase());
    }

    public String getDisplayName() {
        return displayName;
    }
}
