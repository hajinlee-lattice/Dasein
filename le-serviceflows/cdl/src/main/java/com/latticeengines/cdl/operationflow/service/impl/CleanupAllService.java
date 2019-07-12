package com.latticeengines.cdl.operationflow.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Component("cleanupAllService")
@Lazy(value = false)
public class CleanupAllService extends MaintenanceOperationService<CleanupAllConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CleanupAllService.class);

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    public Map<String, Long> invoke(CleanupAllConfiguration config) {
        Map<String, Long> report;
        try {
            report = getReportInfo(config.getCustomerSpace(), config.getEntity());
        } catch (RuntimeException e) {
            log.error("Cannot get delete report for CleanupAll job!");
            report = new HashMap<>();
        }
        String customerSpace = config.getCustomerSpace();
        BusinessEntity entity = config.getEntity();
        if (CleanupOperationType.ALLDATA.equals(config.getCleanupOperationType())) {
            log.info(String.format("begin clean up cdl data for CustomerSpace %s", customerSpace));
            deleteData(config, entity, customerSpace);
        } else if (CleanupOperationType.ALLDATAANDATTRCONFIG.equals(config.getCleanupOperationType())) {
            log.info(String.format("begin clean up cdl data and attr config for CustomerSpace %s", customerSpace));
            deleteData(config, entity, customerSpace);
            deleteAttrConfig(entity, customerSpace);
        } else if (CleanupOperationType.ALLDATAANDMETADATA.equals(config.getCleanupOperationType())) {
            log.info(String.format("begin clean up cdl data and metadata for CustomerSpace %s", customerSpace));
            deleteData(config, entity, customerSpace);
            deleteMetadata(entity, customerSpace);
        } else if (CleanupOperationType.ALLATTRCONFIG.equals(config.getCleanupOperationType())) {
            deleteAttrConfig(entity, customerSpace);
        } else if (CleanupOperationType.ALL.equals(config.getCleanupOperationType())) {
            log.info(String.format("begin clean up cdl data, metadata and attrconfig for CustomerSpace %s",
                    customerSpace));
            deleteData(config, entity, customerSpace);
            deleteMetadata(entity, customerSpace);
            deleteAttrConfig(entity, customerSpace);
        }
        return report;
    }

    private void deleteData(CleanupAllConfiguration config, BusinessEntity entity, String customerSpace) {
        log.info(String.format("begin clean up cdl data of CustomerSpace %s", customerSpace));
        if (entity == null) {
            cleanupRedshift(config.getCustomerSpace(),
                    Arrays.asList(BusinessEntity.Account.getBatchStore(), BusinessEntity.Contact.getBatchStore(),
                            BusinessEntity.Product.getBatchStore(), TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));

            cleanupS3(config.getCustomerSpace(),
                    Arrays.asList(BusinessEntity.Account.getBatchStore(), BusinessEntity.Contact.getBatchStore(),
                            BusinessEntity.Product.getBatchStore(), TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));

            dataCollectionProxy.resetTable(config.getCustomerSpace(), BusinessEntity.Account.getBatchStore());
            dataCollectionProxy.resetTable(config.getCustomerSpace(), BusinessEntity.Contact.getBatchStore());
            dataCollectionProxy.resetTable(config.getCustomerSpace(), BusinessEntity.Product.getBatchStore());
            dataCollectionProxy.resetTable(config.getCustomerSpace(), TableRoleInCollection.ConsolidatedRawTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            dataFeedProxy.resetImport(customerSpace);
        } else if (entity == BusinessEntity.Transaction) {
            cleanupRedshift(config.getCustomerSpace(),
                    Arrays.asList(TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));
            cleanupS3(config.getCustomerSpace(),
                    Arrays.asList(TableRoleInCollection.ConsolidatedRawTransaction,
                            TableRoleInCollection.ConsolidatedDailyTransaction,
                            TableRoleInCollection.ConsolidatedPeriodTransaction),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));
            dataCollectionProxy.resetTable(config.getCustomerSpace(), TableRoleInCollection.ConsolidatedRawTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else if (entity == BusinessEntity.Account || entity == BusinessEntity.Contact
                || entity == BusinessEntity.Product) {
            cleanupRedshift(config.getCustomerSpace(), Arrays.asList(config.getEntity().getBatchStore()),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));
            cleanupS3(config.getCustomerSpace(), Arrays.asList(config.getEntity().getBatchStore()),
                    Arrays.asList(DataCollection.Version.Blue, DataCollection.Version.Green));
            dataCollectionProxy.resetTable(config.getCustomerSpace(), config.getEntity().getBatchStore());
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else {
            log.info(String.format("current Business entity is %s;", entity.name()));
            throw new RuntimeException(String.format("current Business entity is %s, unsupported", entity.name()));
        }

    }

    private void deleteMetadata(BusinessEntity entity, String customerSpace) {
        log.info(String.format("begin clean up cdl metadata of CustomerSpace %s", customerSpace));
        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace);
        List<DataFeedTask> tasks = dataFeed.getTasks();
        for (DataFeedTask task : tasks) {
            Table dataTable = task.getImportData();
            Table templateTable = task.getImportTemplate();
            if (entity == null) {
                if (dataTable != null) {
                    metadataProxy.deleteTable(customerSpace, dataTable.getName());
                }
                metadataProxy.deleteImportTable(customerSpace, templateTable.getName());
            } else if (entity.name().equals(task.getEntity())) {
                if (dataTable != null) {
                    metadataProxy.deleteTable(customerSpace, dataTable.getName());
                }
                metadataProxy.deleteImportTable(customerSpace, templateTable.getName());
            }
        }
    }

    private void deleteAttrConfig(BusinessEntity entity, String customerSpace) {
        log.info(String.format("begin to clean up attr config of CustomerSpace %s", customerSpace));
        cdlAttrConfigProxy.removeAttrConfigByTenantAndEntity(customerSpace, entity);
    }

    private Map<String, Long> getReportInfo(String customerSpace, BusinessEntity entity) {
        Map<String, Long> result = new HashMap<>();
        if (entity == null) {
            Table account = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Account.getBatchStore());
            result.put(BusinessEntity.Account.name(), getTableDataLines(account));
            Table contact = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Contact.getBatchStore());
            result.put(BusinessEntity.Contact.name(), getTableDataLines(contact));
            Table product = dataCollectionProxy.getTable(customerSpace, BusinessEntity.Product.getBatchStore());
            result.put(BusinessEntity.Product.name(), getTableDataLines(product));
            Table rawTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedRawTransaction);
            Table dailyTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            Table periodTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            result.put(BusinessEntity.Transaction.name(), getTableDataLines(rawTransaction)
                    + getTableDataLines(dailyTransaction) + getTableDataLines(periodTransaction));
        } else if (entity == BusinessEntity.Transaction) {
            Table rawTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedRawTransaction);
            Table dailyTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            Table periodTransaction = dataCollectionProxy.getTable(customerSpace,
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            result.put(BusinessEntity.Transaction.name(), getTableDataLines(rawTransaction)
                    + getTableDataLines(dailyTransaction) + getTableDataLines(periodTransaction));
        } else {
            Table table = dataCollectionProxy.getTable(customerSpace, entity.getBatchStore());
            result.put(entity.name(), getTableDataLines(table));
        }
        return result;
    }

    private Long getTableDataLines(Table table) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            if (!extract.getPath().endsWith("avro")) {
                paths.add(extract.getPath() + "/*.avro");
            } else {
                paths.add(extract.getPath());
            }
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
    }

    private void cleanupRedshift(String customSpace, List<TableRoleInCollection> roles,
                                 List<DataCollection.Version> versions) {
        try {
            versions.forEach(version -> {
                roles.forEach(role -> {
                    Table table = dataCollectionProxy.getTable(customSpace, role, version);
                    if (table != null) {
                        List<String> redshiftTables = redshiftService.getTables(table.getName());
                        if (CollectionUtils.isNotEmpty(redshiftTables)) {
                            redshiftTables.forEach(redshiftTable -> redshiftService.dropTable(redshiftTable));
                        }
                    }
                });
            });
        } catch (Exception e) {
            log.error(String.format("Cannot cleanup redshift tables for %s", customSpace));
        }
    }

    private void cleanupS3(String customSpace, List<TableRoleInCollection> roles,
                                 List<DataCollection.Version> versions) {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        try {
            versions.forEach(version -> {
                roles.forEach(role -> {
                    Table table = dataCollectionProxy.getTable(customSpace, role, version);
                    if (table != null) {
                        List<Extract> extracts = table.getExtracts();
                        if (CollectionUtils.isEmpty(extracts) || StringUtils.isBlank(extracts.get(0).getPath())) {
                            log.warn("Can not find extracts of the table=" + table.getName() + " for tenant=" + customSpace);
                            return;
                        }
                        String srcDir = pathBuilder.getFullPath(extracts.get(0).getPath());
                        String tenantId = CustomerSpace.parse(customSpace).getTenantId();
                        String tgtDir = pathBuilder.convertAtlasTableDir(srcDir, podId, tenantId, s3Bucket);
                        String prefix = tgtDir.substring(tgtDir.indexOf(s3Bucket) + s3Bucket.length() + 1);
                        s3Service.cleanupPrefix(s3Bucket, prefix);
                    } else {
                        log.warn("Cannot find table for table role: " + role.name());
                    }
                });
            });
        } catch (Exception e) {
            log.error(String.format("Cannot cleanup s3 tables for %s", customSpace));
        }
    }

}
