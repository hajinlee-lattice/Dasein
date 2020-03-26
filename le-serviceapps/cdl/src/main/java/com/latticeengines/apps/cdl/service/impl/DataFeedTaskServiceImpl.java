package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.RollbackException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.metadata.service.MetadataService;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskServiceImpl.class);
    private static final String SYSTEM_SPLITTER = "_";
    private static final String UNIQUE_NAME_PATTERN = "Task_%s";

    @Inject
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private MetadataService mdService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Override
    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        dataFeedTask.setDataFeed(dataFeed);
        if (StringUtils.isNotBlank(dataFeedTask.getImportSystemName())) {
            dataFeedTask.setImportSystem(
                    s3ImportSystemService.getS3ImportSystem(customerSpace, dataFeedTask.getImportSystemName()));
        }
        if (StringUtils.isEmpty(dataFeedTask.getTaskUniqueName())) {
            dataFeedTask.setTaskUniqueName(generateTaskUniqueName(dataFeed));
        }
        dataFeedTaskEntityMgr.create(dataFeedTask);
    }

    @Override
    public void createOrUpdateDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
            String tableName) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        DataFeedTask dataFeedTask = dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed);
        if (dataFeedTask == null) {
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setDataFeed(dataFeed);
            Table metaData = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedTask.setImportTemplate(metaData);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity);
            dataFeedTask.setFeedType(dataFeedType);
            dataFeedTask.setSource(source);
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setTaskUniqueName(generateTaskUniqueName(dataFeed));
            dataFeedTaskEntityMgr.create(dataFeedTask);
        } else {
            if (!dataFeedTask.getImportTemplate().getName().equals(tableName)) {
                Table metaData = mdService.getTable(CustomerSpace.parse(customerSpace), tableName, true);
                dataFeedTask.setImportTemplate(metaData);
                dataFeedTask.setStatus(DataFeedTask.Status.Updated);
                dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask, false);
                mdService.updateImportTableUpdatedBy(CustomerSpace.parse(customerSpace), dataFeedTask.getImportTemplate());
            }
        }
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, entity, dataFeed);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, dataFeed);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, Long taskId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(taskId);
    }

    @Override
    public DataFeedTask getDataFeedTask(String customerSpace, String uniqueId) {
        return dataFeedTaskEntityMgr.getDataFeedTask(uniqueId);
    }

    @Override
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity) {
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (datafeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTaskWithSameEntity(entity, datafeed);
    }

    @Override
    public List<DataFeedTask> getDataFeedTaskWithSameEntityExcludeOne(String customerSpace, String entity,
                                                                      String excludeSource, String excludeFeedType) {
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (datafeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTaskWithSameEntityExcludeOne(entity, datafeed,
                excludeSource, excludeFeedType);
    }

    @Override
    public List<DataFeedTask> getDataFeedTaskByUniqueIds(String customerSpace, List<String> uniqueIds) {
        if (CollectionUtils.isEmpty(uniqueIds)) {
            return null;
        } else {
            return dataFeedTaskEntityMgr.getDataFeedTaskByUniqueIds(uniqueIds);
        }
    }

    @Override
    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask, boolean updateTaskOnly) {
        DatabaseUtils.retry("Update DataFeedTask", 10,
                RollbackException.class, "RollbackException detected when", null,
                input -> dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask, updateTaskOnly));
        mdService.updateImportTableUpdatedBy(CustomerSpace.parse(customerSpace), dataFeedTask.getImportTemplate());
    }

    @Override
    public void updateS3ImportStatus(String customerSpace, String source, String dataFeedType, DataFeedTask.S3ImportStatus status) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (dataFeed == null) {
            log.error(String.format("Cannot update import status for source: %s, feedType: %s (DataFeed null)",
                    source, dataFeedType));
            return;
        }
        DataFeedTask task = dataFeedTaskEntityMgr.getDataFeedTask(source, dataFeedType, dataFeed);
        if (task == null) {
            log.error(String.format("Cannot update import status for source: %s, feedType: %s (DataFeedTask null)",
                    source, dataFeedType));
            return;
        }
        if (!status.equals(task.getS3ImportStatus())) {
            task.setS3ImportStatus(status);
            dataFeedTaskEntityMgr.updateDataFeedTask(task, true);
        }
    }

    @Override
    public void updateS3ImportStatus(String customerSpace, String uniqueId, DataFeedTask.S3ImportStatus status) {
        DataFeedTask task = dataFeedTaskEntityMgr.getDataFeedTask(uniqueId);
        if (task == null) {
            log.error(String.format("Cannot update import status for DataFeedTask: %s", uniqueId));
            return;
        }
        if (!status.equals(task.getS3ImportStatus())) {
            task.setS3ImportStatus(status);
            dataFeedTaskEntityMgr.updateDataFeedTask(task, true);
        }
    }

    @Override
    public List<String> registerExtract(String customerSpace, String taskUniqueId, String tableName, Extract extract) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        DataFeed dataFeed = dataFeedTask.getDataFeed();
        if (dataFeed.getStatus() == DataFeed.Status.Initing) {
            // log.info("Skip registering extract for feed in initing state");
            return Collections.emptyList();
        }
        return dataFeedTaskEntityMgr.registerExtract(dataFeedTask, tableName, extract);
    }

    @Override
    public List<String> registerExtracts(String customerSpace, String taskUniqueId, String tableName, List<Extract> extracts) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        DataFeed dataFeed = dataFeedTask.getDataFeed();
        if (dataFeed.getStatus() == DataFeed.Status.Initing) {
            // log.info("Skip registering extract for feed in initing state");
            return Collections.emptyList();
        }
        return dataFeedTaskEntityMgr.registerExtracts(dataFeedTask, tableName, extracts);
    }

    @Override
    public void addTableToQueue(String customerSpace, String taskUniqueId, String tableName) {
        dataFeedTaskEntityMgr.addTableToQueue(taskUniqueId, tableName);
    }

    @Override
    public void addTablesToQueue(String customerSpace, String taskUniqueId, List<String> tableNames) {
        dataFeedTaskEntityMgr.addTablesToQueue(taskUniqueId, tableNames);
    }

    @Override
    public List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType,
            String entity) {
        DataFeedTask datafeedTask = getDataFeedTask(customerSpace, source, dataFeedType, entity);
        return dataFeedTaskEntityMgr.getExtractsPendingInQueue(datafeedTask);
    }

    @Override
    public void resetImport(String customerSpaceStr, DataFeedTask datafeedTask) {
        dataFeedTaskEntityMgr.clearTableQueuePerTask(datafeedTask);
    }

    @Override
    public List<Table> getTemplateTables(String customerSpace, String entity) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        List<DataFeedTask> datafeedTasks = dataFeedTaskEntityMgr.getDataFeedTaskWithSameEntity(entity, dataFeed);
        List<Table> tables = new LinkedList<>();
        if (!CollectionUtils.isEmpty(datafeedTasks)) {
            for (DataFeedTask dataFeedTask : datafeedTasks) {
                tables.add(dataFeedTask.getImportTemplate());
            }
        }
        return tables;
    }

    @Override
    public S3ImportSystem getImportSystemByTaskId(String customerSpace, String taskUniqueId) {
        DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, taskUniqueId);
        if (dataFeedTask == null) {
            return null;
        }
        String systemName = getSystemNameFromFeedType(dataFeedTask.getFeedType());
        if (StringUtils.isEmpty(systemName)) {
            return null;
        }
        return s3ImportSystemService.getS3ImportSystem(customerSpace, systemName);
    }

    @Override
    public List<String> getTemplatesBySystemPriority(String customerSpace, String entity, boolean highestFirst) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        List<DataFeedTask> dataFeedTasks =
                dataFeedTaskEntityMgr.getDataFeedTaskWithSameEntity(entity, dataFeed);
        Comparator<S3ImportSystem> comparing = highestFirst ? Comparator.comparing(S3ImportSystem::getPriority) :
                Comparator.comparing(S3ImportSystem::getPriority).reversed();
        if (!CollectionUtils.isEmpty(dataFeedTasks)) {
            List<S3ImportSystem> allSystems = s3ImportSystemService.getAllS3ImportSystem(customerSpace);
            List<String> orderedSystem = allSystems.stream()
                    .filter(Objects::nonNull)
                    .sorted(comparing)
                    .map(S3ImportSystem::getName)
                    .collect(Collectors.toList());
            List<Pair<String, String>> templatePair = new ArrayList<>();
            for(DataFeedTask dataFeedTask : dataFeedTasks) {
                if (dataFeedTask.getImportTemplate() != null) {
                    String systemName = getSystemNameFromFeedType(dataFeedTask.getFeedType());
                    if (StringUtils.isNotEmpty(systemName)) {
                        templatePair.add(Pair.of(systemName, dataFeedTask.getImportTemplate().getName()));
                    }
                }
            }
            templatePair.sort(Comparator.comparing(pair -> orderedSystem.indexOf(pair.getLeft())));
            return templatePair.stream().map(Pair::getRight).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public DataFeedTask getDataFeedTaskBySource(String customerSpace, String sourceId) {
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (datafeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTask(datafeed, sourceId);
    }

    @Override
    public void setDataFeedTaskDelete(String customerSpace, Long pid, Boolean deleted) {
        dataFeedTaskEntityMgr.setDeleted(pid, deleted);
    }

    @Override
    public void setDataFeedTaskS3ImportStatus(String customerSpace, Long pid, DataFeedTask.S3ImportStatus status) {
        dataFeedTaskEntityMgr.setS3ImportStatusBySource(pid, status);
    }
    private String getSystemNameFromFeedType(String feedType) {
        if (StringUtils.isNotEmpty(feedType) && feedType.contains(SYSTEM_SPLITTER)) {
            return feedType.substring(0, feedType.lastIndexOf(SYSTEM_SPLITTER));
        }
        return null;
    }

    private String generateTaskUniqueName(DataFeed dataFeed) {
        String taskName;
        do {
            taskName = String.format(UNIQUE_NAME_PATTERN, RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        } while (dataFeedTaskEntityMgr.getDataFeedTaskByTaskName(taskName, dataFeed, Boolean.FALSE) != null);
        return taskName;
    }
}
