package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum CDLExternalSystemName {
    Marketo("Marketo", Arrays.asList(BusinessEntity.Contact)), //
    Eloqua("Eloqua", Arrays.asList(BusinessEntity.Contact)), //
    Salesforce("Salesforce", Arrays.asList(BusinessEntity.Account)), //
    Facebook("Facebook", Arrays.asList(BusinessEntity.Contact)), //
    LinkedIn("LinkedIn", Arrays.asList(BusinessEntity.Contact, BusinessEntity.Account)), //
    GoogleAds("GoogleAds", Arrays.asList(BusinessEntity.Contact)), //
    AWS_S3("AWS S3", Arrays.asList(BusinessEntity.Contact, BusinessEntity.Account)), //
    Outreach("Outreach", Arrays.asList(BusinessEntity.Contact, BusinessEntity.Account)), //
    Adobe_Audience_Mgr("Adobe Audience Manager", Arrays.asList(BusinessEntity.Account)), //
    MediaMath("MediaMath", Arrays.asList(BusinessEntity.Account)), //
    TradeDesk("TradeDesk", Arrays.asList(BusinessEntity.Account)), //
    Verizon_Media("Verizon Media", Arrays.asList(BusinessEntity.Account)), //
    Google_Display_N_Video_360("Google Display & Video 360", Arrays.asList(BusinessEntity.Account)), //
    AppNexus("AppNexus", Arrays.asList(BusinessEntity.Account)), //
    Others("Others", Arrays.asList());

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

    private static Map<BusinessEntity, List<CDLExternalSystemName>> entityMap = new HashMap<>();

    static {
        for (CDLExternalSystemName sysName : CDLExternalSystemName.values()) {
            for (BusinessEntity entity : sysName.getLaunchToEntities()) {
                if (entityMap.containsKey(entity)) {
                    List<CDLExternalSystemName> retrievedList = entityMap.get(entity);
                    retrievedList.add(sysName);
                    entityMap.put(entity, retrievedList);
                } else {
                    entityMap.put(entity, new ArrayList<CDLExternalSystemName>(Arrays.asList(sysName)));
                }
            }
        }
    }

    private String displayName;
    private List<BusinessEntity> canLaunchToEntities;

    CDLExternalSystemName(String displayName, List<BusinessEntity> canLaunchToEntities) {
        this.displayName = displayName;
        this.canLaunchToEntities = canLaunchToEntities;
    }

    public String getDisplayName() {
        return displayName;
    }

    public List<BusinessEntity> getLaunchToEntities() {
        return canLaunchToEntities;
    }

    public static CDLExternalSystemName getSystemNameEnum(String systemName) {
        if (systemName == null) {
            return null;
        }
        return map.get(systemName.toUpperCase());
    }

    public static Map<BusinessEntity, List<CDLExternalSystemName>> getEntityMap() {
        return entityMap;
    }
}
