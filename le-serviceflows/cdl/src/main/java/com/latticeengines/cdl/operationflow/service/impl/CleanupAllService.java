package com.latticeengines.cdl.operationflow.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.operationflow.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("cleanupAllService")
public class CleanupAllService  extends MaintenanceOperationService<CleanupAllConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(CleanupAllService.class);
    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private MetadataProxy metadataProxy;

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
        log.info(String.format("begin clean up cdl data of CustomerSpace %s", customerSpace));
        if (entity == null) {
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
            dataCollectionProxy.resetTable(config.getCustomerSpace(), TableRoleInCollection.ConsolidatedRawTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedDailyTransaction);
            dataCollectionProxy.resetTable(config.getCustomerSpace(),
                    TableRoleInCollection.ConsolidatedPeriodTransaction);
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else if (entity == BusinessEntity.Account || entity == BusinessEntity.Contact
                || entity == BusinessEntity.Product) {
            dataCollectionProxy.resetTable(config.getCustomerSpace(), config.getEntity().getBatchStore());
            dataFeedProxy.resetImportByEntity(customerSpace, entity.name());
        } else {
            log.info(String.format("current Business entity is %s;", entity.name()));
            throw new RuntimeException(String.format("current Business entity is %s, unsupported", entity.name()));
        }

        log.info("Start cleanup all operation!");
        if (config.getCleanupOperationType() == CleanupOperationType.ALL) {
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
        return report;
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
            Table rawTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
            Table dailyTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedDailyTransaction);
            Table periodTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedPeriodTransaction);
            result.put(BusinessEntity.Transaction.name(), getTableDataLines(rawTransaction)
                    + getTableDataLines(dailyTransaction) + getTableDataLines(periodTransaction));
        } else if (entity == BusinessEntity.Transaction) {
            Table rawTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedRawTransaction);
            Table dailyTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedDailyTransaction);
            Table periodTransaction = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedPeriodTransaction);
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
        for (Extract extract : table.getExtracts()) {
            lines += extract.getProcessedRecords();
        }
        return lines;
    }
}
