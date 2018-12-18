package com.latticeengines.playmaker.service;

import java.util.List;
import java.util.Map;

public interface LpiPMAccountExtension {

    List<Map<String, Object>> getAccountExtensions(long start, long offset, long maximum, List<String> accountIds,
            String filterBy, Long recStart, String columns, boolean hasSfdcContactId, Map<String, String> orgInfo);

    long getAccountExtensionCount(long start, List<String> accountIds, String filterBy, Long recStart,
            Map<String, String> orgInfo);

    List<Map<String, Object>> getAccountExtensionSchema(String customerSpace);

    List<Map<String, Object>> getContactExtensionSchema(String customerSpace);

    int getAccountExtensionColumnCount(String customerSpace);

    int getContactExtensionColumnCount(String customerSpace);

    List<Map<String,Object>> getAccountIdsByRecommendationsInfo(boolean latest, Long recStart, long offset, long max,
            Map<String, String> orgInfo);
}
