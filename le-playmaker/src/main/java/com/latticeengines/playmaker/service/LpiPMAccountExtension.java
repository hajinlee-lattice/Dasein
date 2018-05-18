package com.latticeengines.playmaker.service;

import java.util.List;
import java.util.Map;

public interface LpiPMAccountExtension {

    List<Map<String, Object>> getAccountExtensions(long start, long offset, long maximum, List<String> accountIds,
            Long recStart, String columns, boolean hasSfdcContactId);

    long getAccountExtensionCount(long start, List<String> accountIds, Long recStart);

    List<Map<String, Object>> getAccountExtensionSchema(String customerSpace);

    List<Map<String, Object>> getContactExtensionSchema(String customerSpace);

    int getAccountExtensionColumnCount(String customerSpace);

    int getContactExtensionColumnCount(String customerSpace);
}
