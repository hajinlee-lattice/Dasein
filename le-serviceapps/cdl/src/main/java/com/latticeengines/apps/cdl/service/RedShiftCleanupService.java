package com.latticeengines.apps.cdl.service;

public interface RedShiftCleanupService {

    boolean removeUnusedTable();

    boolean removeUnusedTableByTenant(String customerspace);

    boolean removeUnusedRedshiftTable();

    void dealUnusedRedshiftTable();
}
