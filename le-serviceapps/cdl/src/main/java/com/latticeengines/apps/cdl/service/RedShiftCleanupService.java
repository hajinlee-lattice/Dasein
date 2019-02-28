package com.latticeengines.apps.cdl.service;

public interface RedShiftCleanupService {

    boolean removeUnusedTableByTenant(String customerspace);

    boolean removeUnusedTables();
}
