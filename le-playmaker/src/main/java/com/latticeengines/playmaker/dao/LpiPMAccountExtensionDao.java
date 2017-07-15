package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

public interface LpiPMAccountExtensionDao {

    List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum, List<String> accountIds,
            Long recStart, String columns, boolean hasSfdcContactId);

    int getAccountExtensionCount(long start, List<String> accountIds, Long recStart);

    List<Map<String, Object>> getAccountExtensionSchema();

    List<Map<String, Object>> getContactExtensionSchema();

    int getAccountExtensionColumnCount();

    int getContactExtensionColumnCount();
}
