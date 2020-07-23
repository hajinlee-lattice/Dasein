package com.latticeengines.metadata.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.MigrateDynamoRequest;
import com.latticeengines.domain.exposed.datafabric.GenericTableActivity;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.migrate.MigrateDynamoWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.metadata.service.DataUnitService;
import com.latticeengines.metadata.service.MetadataMigrateDynamoService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("metadataMigrateDynamoServiceImpl")
public class MetadataMigrateDynamoServiceImpl implements MetadataMigrateDynamoService {

    private static final Logger log = LoggerFactory.getLogger(MetadataMigrateDynamoServiceImpl.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Inject
    private DataUnitService dataUnitService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    private CDLProxy cdlProxy;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Value("${eai.export.dynamo.timeline.signature}")
    private String timelineSignature;

    @Value("${common.adminconsole.url}")
    private String quartzMicroserviceHostPort;

    @Value("${common.microservice.url}")
    private String microserviceHostPort;

    @Value("${common.quartz.stack.flag:false}")
    private boolean isQuartzStack;

    @Value("${metadata.dynamo.migrate.size}")
    private int migrateSize;

    private final int batchSize = 5;

    private static final List<String> types = Collections.singletonList("migrateDynamoWorkflow");
    private static final List<String> jobStatuses = Lists.newArrayList(JobStatus.RUNNING.getName(),
            JobStatus.PENDING.getName(), JobStatus.ENQUEUED.getName());

    @PostConstruct
    public void initialize() {
        if (isQuartzStack) {
            cdlProxy = new CDLProxy(quartzMicroserviceHostPort);
        } else {
            cdlProxy = new CDLProxy(microserviceHostPort);
        }
    }

    private Tenant getTenant(Map<String, Tenant> tenantMap, String tenantId) {
        Tenant tenant = tenantMap.get(tenantId);
        if (tenant == null) {
            tenant = tenantEntityMgr.findByTenantId(tenantId);
            tenantMap.put(tenantId, tenant);
        }
        return tenant;
    }

    private List<String> getTableNames(Map<String, List<String>> tableMap, String tenantId) {
        List<String> tableNames = tableMap.get(tenantId);
        if (CollectionUtils.isEmpty(tableNames)) {
            tableNames = dataCollectionProxy.getTableNames(tenantId, null);
            tableMap.put(tenantId, tableNames);
        }
        return tableNames;
    }

    @Override
    public Boolean migrateDynamo() {
        List<DataUnit> dataUnits = dataUnitService.findByStorageType(DataUnit.StorageType.Dynamo);
        log.info(String.format("Migrate dynamo table task started and total dynamo data unit count is %d.", dataUnits.size()));
        Map<String, Tenant> tenantMap = new HashMap<>();
        Map<String, List<Job>> jobMap = new HashMap<>();
        Map<String, List<String>> tableMap = new HashMap<>();
        int totalSize = 0;
        Map<String, Map<String, List<String>>> tableNamesNeedToMigrate = new HashMap<>();
        for (DataUnit dataUnit : dataUnits) {
            if (totalSize >= migrateSize) {
                log.info("Already found {} dynamo data units in progress of migration.", migrateSize);
                break;
            }
            DynamoDataUnit dynamoDataUnit = (DynamoDataUnit) dataUnit;
            String entityClass = dynamoDataUnit.getEntityClass();
            if (StringUtils.isEmpty(entityClass)) {
                entityClass = GenericTableEntity.class.getCanonicalName();
            }
            if (needMigrate(dynamoDataUnit)) {
                String linkedTenantName = dynamoDataUnit.getLinkedTenant();
                if (StringUtils.isEmpty(linkedTenantName)) {
                    String shortTenantId = dynamoDataUnit.getTenant();
                    CustomerSpace customerSpace = CustomerSpace.parse(shortTenantId);
                    String tenantId = customerSpace.toString();
                    Tenant tenant = getTenant(tenantMap, tenantId);
                    if (tenant != null) {
                        // only few dynamo test records so we can migrate them with simple code if needed
                        if (!tenant.getName().startsWith("LETest")) {
                            String linkedTableName = dynamoDataUnit.getLinkedTable();
                            if (StringUtils.isEmpty(linkedTableName)) {
                                String tableName = dynamoDataUnit.getName();
                                List<String> tableNames = getTableNames(tableMap, tenantId);
                                // if metadata collection table exists, metadata table should exist
                                if (tableNames.contains(tableName)) {
                                    if (!isTableInMigration(tenantId, tableName, jobMap)) {
                                        totalSize++;
                                        tableNamesNeedToMigrate.putIfAbsent(entityClass, new HashMap<>());
                                        tableNamesNeedToMigrate.get(entityClass).putIfAbsent(tenantId, new ArrayList<>());
                                        tableNamesNeedToMigrate.get(entityClass).get(tenantId).add(tableName);
                                    }
                                } else {
                                    log.info("Table info can't be found in dynamo data unit with name {} and tenant {}.",
                                            dynamoDataUnit.getTenant(), dynamoDataUnit.getName());
                                    handleTableNotFoundDataUnit(tenant, dynamoDataUnit);
                                }
                            } else {
                                log.info("Linked table info found in dynamo data unit with name {} and tenant {}.",
                                        dynamoDataUnit.getTenant(), dynamoDataUnit.getName());
                                handleLinkedTableDataUnit(tenant, dynamoDataUnit);
                            }
                        }
                    } else {
                        log.info("Tenant info can't be found in dynamo data unit with name {} and tenant {}.",
                                dynamoDataUnit.getTenant(), dynamoDataUnit.getName());
                    }
                } else {
                    log.info("Linked tenant info found in dynamo data unit with name {} and tenant {}, will not migrate it.",
                            dynamoDataUnit.getTenant(), dynamoDataUnit.getName());
                }
            }
        }
        if (totalSize == 0) {
            log.info("No dynamo data unit needs to migrate.");
        } else {
            migrateTables(tableNamesNeedToMigrate);
        }
        return true;
    }

    private boolean needMigrate(DynamoDataUnit dynamoDataUnit) {
        String entityClass = dynamoDataUnit.getEntityClass();
        if (StringUtils.isEmpty(entityClass)) {
            return !signature.equals(dynamoDataUnit.getSignature());
        }
        if (entityClass.equals(GenericTableActivity.class.getCanonicalName())) {
            return !timelineSignature.equals(dynamoDataUnit.getSignature());
        } else if (entityClass.equals(GenericTableEntity.class.getCanonicalName())) {
            return !signature.equals(dynamoDataUnit.getSignature());
        }
        return false;
    }

    private String getSignature(String entityClass) {
        if (entityClass.equals(GenericTableActivity.class.getCanonicalName())) {
            return timelineSignature;
        } else if (entityClass.equals(GenericTableEntity.class.getCanonicalName())) {
            return signature;
        }
        return signature;
    }

    // just update data unit to make it safe
    private void updateDataUnit(DynamoDataUnit dynamoDataUnit) {
        dataUnitService.updateSignature(dynamoDataUnit, signature);
    }

    private void handleTableNotFoundDataUnit(Tenant tenant, DynamoDataUnit dynamoDataUnit) {
        Tenant preTenant = MultiTenantContext.getTenant();
        MultiTenantContext.setTenant(tenant);
        updateDataUnit(dynamoDataUnit);
        MultiTenantContext.setTenant(preTenant);
    }

    private void handleLinkedTableDataUnit(Tenant tenant, DynamoDataUnit dynamoDataUnit) {
        Tenant preTenant = MultiTenantContext.getTenant();
        MultiTenantContext.setTenant(tenant);
        DataUnit dataUnit = dataUnitService.findByNameTypeFromReader(dynamoDataUnit.getLinkedTable(), DataUnit.StorageType.Dynamo);
        if (dataUnit != null) {
            DynamoDataUnit linkedDynamoDataUnit = (DynamoDataUnit) dataUnit;
            if (signature.equals(linkedDynamoDataUnit.getSignature())) {
                updateDataUnit(dynamoDataUnit);
            }
        } else {
            updateDataUnit(dynamoDataUnit);
        }
        MultiTenantContext.setTenant(preTenant);
    }

    private void migrateTables(Map<String, Map<String, List<String>>> tableNamesNeedToMigrateMap) {
        for (Map.Entry<String, Map<String, List<String>>> entry : tableNamesNeedToMigrateMap.entrySet()) {
            String entityClass = entry.getKey();
            String signature = getSignature(entityClass);
            Map<String, List<String>> tableNamesNeedToMigrate = entry.getValue();
            if (MapUtils.isNotEmpty(tableNamesNeedToMigrate)) {
                for (Map.Entry<String, List<String>> tableEntry : tableNamesNeedToMigrate.entrySet()) {
                    String tenantId = tableEntry.getKey();
                    List<String> tableNames = tableEntry.getValue();
                    List<List<String>> batchList = Lists.partition(tableNames, batchSize);
                    for (List<String> subTableNames : batchList) {
                        log.info("Tables {} in tenant {} will be migrated to signature {}.", subTableNames, tenantId, getSignature(entityClass));
                        migrateTable(entityClass, tenantId, subTableNames, signature);
                        SleepUtils.sleep(TimeUnit.SECONDS.toMillis(30));
                    }
                }
            }
        }
    }

    private void migrateTable(String entityClass, String tenantId, List<String> tableNames, String signature) {
        try {
            MigrateDynamoRequest migrateDynamoRequest = new MigrateDynamoRequest();
            migrateDynamoRequest.setTableNames(tableNames);
            migrateDynamoRequest.setEntityClass(entityClass);
            migrateDynamoRequest.setSignature(signature);
            cdlProxy.submitMigrateDynamoJob(tenantId, migrateDynamoRequest);
        } catch (Exception e) {
            log.error("Failed to submit migrate dynamo job with exception {}.", e.getMessage());
        }

    }

    // check if table is in progress of migration
    private boolean isTableInMigration(String tenantId, String tableName, Map<String, List<Job>> jobMap) {
        List<Job> jobs = jobMap.get(tenantId);
        if (jobs == null) {
            jobs = workflowProxy.getJobs(null, types, jobStatuses, false, tenantId);
            jobMap.put(tenantId, jobs);
        }
        for (Job job : jobs) {
            Map<String, String> inputs = job.getInputs();
            if (MapUtils.isNotEmpty(inputs)) {
                String tableNamesInJson = inputs.get(MigrateDynamoWorkflowConfiguration.IMPORT_TABLE_NAMES);
                if (StringUtils.isNotEmpty(tableNamesInJson)) {
                    List<String> tableNames = JsonUtils.deserialize(tableNamesInJson, List.class);
                    if (tableNames.contains(tableName)) {
                        log.info("Dynamo data unit unit with name {} and tenant {} is in progress of migration in job {}.",
                                tableName, tenantId, job.getApplicationId());
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
