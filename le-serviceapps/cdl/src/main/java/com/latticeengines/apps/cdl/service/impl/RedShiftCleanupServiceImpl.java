package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataFeedEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.RedShiftCleanupService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("redShiftCleanupService")
public class RedShiftCleanupServiceImpl implements RedShiftCleanupService {

    private static final Logger log = LoggerFactory.getLogger(RedShiftCleanupService.class);

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataFeedEntityMgr dataFeedEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Override
    public boolean removeUnusedTable() {
        List<SimpleDataFeed> allSimpleDataFeeds = dataFeedEntityMgr.getAllSimpleDataFeeds();
        if (CollectionUtils.isEmpty(allSimpleDataFeeds)) {
            return true;
        }
        for (SimpleDataFeed simpleDataFeed : allSimpleDataFeeds) {
            Tenant tenant = simpleDataFeed.getTenant();
            this.dropTableByTenant(tenant);
        }
        return true;
    }

    public boolean removeUnusedTableByTenant(String customerSpace) {
        Tenant tenant = tenantEntityMgr.findByTenantId(customerSpace);
        log.info("tenant is :" + tenant.getName());
        this.dropTableByTenant(tenant);
        return true;
    }

    private void dropTableByTenant(Tenant tenant) {
        MultiTenantContext.setTenant(tenant);
        log.info("tenant name is : " + tenant.getName());
        String customerspace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection dataCollection = dataCollectionService.getDefaultCollection(customerspace);
        DataCollection.Version activeVersion = dataCollection.getVersion();
        DataCollection.Version inactiveVersion = activeVersion.complement();
        // 1. get all inused tablename in this tenant_id
        Set<String> tableNames = new HashSet<>();
        tableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, activeVersion));
        tableNames.addAll(dataCollectionService.getTableNames(customerspace, dataCollection.getName(), null, inactiveVersion));
        log.info("inuse tableNames:" + tableNames.toString());
        // 2. get all tablename in this tenant_id on redshift
        List<String> redshift_tableNames = redshiftService.getTables(tenant.getName());
        log.info("redshift tablename under tenant is :" + redshift_tableNames.toString());
        // 3.drop table
        dropTable(redshift_tableNames, new ArrayList<>(tableNames));
    }

    private void dropTable(List<String> redshift_table, List<String> data_table) {
        redshift_table.removeAll(data_table);
        if (CollectionUtils.isEmpty(redshift_table)) {
            return;
        }
        for (String simpleTable : redshift_table) {
            log.info("drop table : " + simpleTable);
            redshiftService.dropTable(simpleTable);
        }
    }

}
