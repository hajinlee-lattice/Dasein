package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;

public enum CDLExternalSystemName {
    Marketo("Marketo"), //
    Eloqua("Eloqua"), //
    Salesforce("Salesforce"), //
    Facebook("Facebook"), //
    LinkedIn("LinkedIn"), //
    GoogleAds("GoogleAds"), //
    AWS_S3("AWS S3"), //
    Outreach("Outreach"), //
    Adobe_Audience_Mgr("Adobe Audience Manager"), //
    MediaMath("MediaMath"), //
    TradeDesk("TradeDesk"), //
    Verizon_Media("Verizon Media"), //
    Google_Display_N_Video_360("Google Display & Video 360"), //
    AppNexus("AppNexus"), //
    Others("Others");

    public static final ImmutableList<CDLExternalSystemName> AD_PLATFORMS = //
            ImmutableList.of(CDLExternalSystemName.LinkedIn, CDLExternalSystemName.GoogleAds, CDLExternalSystemName.Facebook);

    public static final ImmutableList<CDLExternalSystemName> LIVERAMP = //
            ImmutableList.of(CDLExternalSystemName.Adobe_Audience_Mgr, CDLExternalSystemName.MediaMath,
                    CDLExternalSystemName.TradeDesk, CDLExternalSystemName.Verizon_Media,
                    CDLExternalSystemName.Google_Display_N_Video_360, CDLExternalSystemName.AppNexus);

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
