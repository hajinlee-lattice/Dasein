package com.latticeengines.testframework.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@Service("cdlTestDataService")
public class CDLTestDataServiceImpl implements CDLTestDataService {

    private static final String S3_DIR = "le-testframework/cdl";
    private static final String S3_VERSION = "1";
    private static final Date DATE = new Date();

    private static final ImmutableMap<BusinessEntity, String> srcTables = ImmutableMap.of( //
            BusinessEntity.Account, "query_test_account2", //
            BusinessEntity.Contact, "query_test_contact2", //
            BusinessEntity.Product, "query_test_product2", //
            BusinessEntity.Transaction, "query_test_transaction2" //
    );

    private final TestArtifactService testArtifactService;
    private final MetadataProxy metadataProxy;
    private final DataCollectionProxy dataCollectionProxy;
    private final RedshiftService redshiftService;

    @Inject
    public CDLTestDataServiceImpl(TestArtifactService testArtifactService, MetadataProxy metadataProxy,
            DataCollectionProxy dataCollectionProxy, RedshiftService redshiftService) {
        this.testArtifactService = testArtifactService;
        this.metadataProxy = metadataProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.redshiftService = redshiftService;
    }

    @Override
    public void populateData(String tenantId) {
        final String shortTenantId = CustomerSpace.parse(tenantId).getTenantId();
        dataCollectionProxy.getDefaultDataCollection(shortTenantId);
        ExecutorService executors = ThreadPoolUtils.getCachedThreadPool("cdl-test-data");
        Set<Future> futures = new HashSet<>();
        futures.add(executors.submit(() -> {
            populateStats(shortTenantId);
            return true;
        }));
        for (BusinessEntity entity : BusinessEntity.values()) {
            futures.add(executors.submit(() -> {
                try (PerformanceTimer timer = new PerformanceTimer("Clone redshift table for " + entity)) {
                    cloneRedshiftTables(shortTenantId, entity);
                }
                return true;
            }));
            futures.add(executors.submit(() -> {
                populateServingStore(shortTenantId, entity);
                return true;
            }));
        }
        while (!futures.isEmpty()) {
            Set<Future> toBeDeleted = new HashSet<>();
            futures.forEach(future -> {
                try {
                    future.get(10, TimeUnit.SECONDS);
                    toBeDeleted.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (Exception e) {
                    throw new RuntimeException("One of the future is failed", e);
                }
            });

            futures.removeAll(toBeDeleted);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void populateStats(String tenantId) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        StatisticsContainer container;
        try {
            InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, "stats_container.json");
            String content = IOUtils.toString(is, Charset.forName("UTF-8"));
            content = content.replace("$$StatsName$$", NamingUtils.timestamp("Stats", DATE));
            ObjectMapper om = new ObjectMapper();
            container = om.readValue(content, StatisticsContainer.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download from S3 and parse stats container", e);
        }
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        container.setVersion(activeVersion);
        dataCollectionProxy.upsertStats(customerSpace, container);
    }

    private void cloneRedshiftTables(String tenantId, BusinessEntity entity) {
        if (srcTables.containsKey(entity)) {
            String srcTable = srcTables.get(entity);
            String tgtTable = servingStoreName(tenantId, entity);
            redshiftService.cloneTable(srcTable, tgtTable);
        }
    }

    private void populateServingStore(String tenantId, BusinessEntity entity) {
        if (Arrays.asList( //
                BusinessEntity.Account, //
                BusinessEntity.Contact, //
                BusinessEntity.Product, //
                BusinessEntity.Transaction //
        ).contains(entity)) {
            String customerSpace = CustomerSpace.parse(tenantId).toString();
            Table table = readTableFromS3(tenantId, entity);
            metadataProxy.createTable(customerSpace, table.getName(), table);
            DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
            dataCollectionProxy.upsertTable(customerSpace, table.getName(), entity.getServingStore(), activeVersion);
        }
    }

    private Table readTableFromS3(String tenantId, BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, role.name() + ".json");
        Table table;
        try {
            String content = IOUtils.toString(is, Charset.forName("UTF-8"));
            content = content.replace("$$TableName$$", servingStoreName(tenantId, entity));
            ObjectMapper om = new ObjectMapper();
            table = om.readValue(content, Table.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse the table json", e);
        }
        table.setTableType(TableType.DATATABLE);
        return table;
    }

    private String servingStoreName(String tenantId, BusinessEntity entity) {
        return NamingUtils.timestamp(tenantId + "_" + entity.name(), DATE);
    }

}
