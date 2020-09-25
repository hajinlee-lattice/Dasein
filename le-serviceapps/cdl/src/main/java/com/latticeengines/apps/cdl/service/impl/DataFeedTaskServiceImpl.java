package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.persistence.RollbackException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.cdl.entitymgr.DataFeedTaskEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.cdl.service.DataFeedTaskService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.SourceInfo;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTaskSummary;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.service.MetadataService;

@Component("dataFeedTaskService")
public class DataFeedTaskServiceImpl implements DataFeedTaskService {

    private static final Logger log = LoggerFactory.getLogger(DataFeedTaskServiceImpl.class);
    private static final String SYSTEM_SPLITTER = "_";
    private static final String UNIQUE_NAME_PATTERN = "Task_%s";
    private static final int MAX_PAGE_SIZE = 100;

    @Inject
    private DataFeedTaskEntityMgr dataFeedTaskEntityMgr;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private MetadataService mdService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;

    @Inject
    private DataCollectionService dataCollectionService;

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
                        templatePair.add(Pair.of(systemName, getDataFeedTaskUniqueName(customerSpace, dataFeedTask)));
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
    public String getTemplateName(String customerSpace, String taskUniqueId) {
        DataFeedTask dataFeedTask = dataFeedTaskEntityMgr.getDataFeedTask(taskUniqueId);
        if (dataFeedTask == null) {
            return StringUtils.EMPTY;
        }
        return getDataFeedTaskUniqueName(customerSpace, dataFeedTask);
    }

    @Override
    public Map<String, String> getTemplateToSystemMap(String customerSpace) {
        Map<String, S3ImportSystem> templateSystemMap = getTemplateToSystemObjectMap(customerSpace);
        if (MapUtils.isNotEmpty(templateSystemMap)) {
            return templateSystemMap.entrySet().stream().map(entry -> Pair.of(entry.getKey(), entry.getValue().getName())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, S3ImportSystem.SystemType> getTemplateToSystemTypeMap(String customerSpace) {
        Map<String, S3ImportSystem> templateSystemMap = getTemplateToSystemObjectMap(customerSpace);
        if (MapUtils.isNotEmpty(templateSystemMap)) {
            return templateSystemMap.entrySet().stream().map(entry -> Pair.of(entry.getKey(),
                    entry.getValue().getSystemType())).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String, S3ImportSystem> getTemplateToSystemObjectMap(String customerSpace) {
        Map<String, DataFeedTask> templateToDataFeedTaskMap = getTemplateToDataFeedTaskMap(customerSpace);
        if (MapUtils.isNotEmpty(templateToDataFeedTaskMap)) {
            return templateToDataFeedTaskMap.values().stream().map(task -> {
                S3ImportSystem importSystem = task.getImportSystem();
                if (StringUtils.isEmpty(task.getImportSystemName())) {
                    String systemName = getSystemNameFromFeedType(task.getFeedType());
                    if (StringUtils.isNotEmpty(systemName)) {
                        importSystem = s3ImportSystemService.getS3ImportSystem(customerSpace,
                                systemName);
                        if (importSystem != null) {
                            task.setImportSystem(importSystem);
                            dataFeedTaskEntityMgr.updateDataFeedTask(task, true);
                        }
                    }
                }
                if (importSystem != null) {
                    importSystem.setTasks(null);
                }
                return Pair.of(getDataFeedTaskUniqueName(customerSpace, task), importSystem);
            }).filter(pair -> pair.getValue() != null).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        }
        return Collections.emptyMap();
    }

    @Override
    public Map<String, List<String>> getSystemNameToUniqueIdsMap(String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        List<DataFeedTaskSummary> allSummaries = dataFeedTaskEntityMgr.getSummaryByDataFeed(customerSpace);
        if (CollectionUtils.isNotEmpty(allSummaries)) {
            Map<String, List<String>> systemToUniqueIdsMap = new HashMap<>();
            allSummaries.forEach(taskSummary -> {
                String systemName = getSystemNameFromFeedType(taskSummary.getFeedType());
                if (StringUtils.isNotEmpty(systemName)) {
                    systemToUniqueIdsMap.putIfAbsent(systemName, new ArrayList<>());
                    systemToUniqueIdsMap.get(systemName).add(taskSummary.getUniqueId());
                }
            });
            return systemToUniqueIdsMap;
        }
        return Collections.emptyMap();
    }

    @Override
    public Map<String, DataFeedTask> getTemplateToDataFeedTaskMap(String customerSpace) {
        DataFeed dataFeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        List<DataFeedTask> allTasks = dataFeedTaskEntityMgr.getDataFeedTaskUnderDataFeed(dataFeed);
        if (CollectionUtils.isNotEmpty(allTasks)) {
            return allTasks.stream().map(task -> Pair.of(getDataFeedTaskUniqueName(customerSpace, task), task)).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        }
        return Collections.emptyMap();
    }

    @Override
    public DataFeedTask getDataFeedTaskByTaskName(String customerSpace, String taskName, Boolean withTemplate) {
        DataFeed datafeed = dataFeedService.getOrCreateDataFeed(customerSpace);
        if (datafeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getDataFeedTaskByTaskName(taskName, datafeed, withTemplate);
    }

    private String getDataFeedTaskUniqueName(String customerSpace, DataFeedTask dataFeedTask) {
        if (StringUtils.isEmpty(dataFeedTask.getTaskUniqueName())) {
            dataFeedTask.setTaskUniqueName(generateTaskUniqueName(dataFeedTask.getDataFeed()));
            dataFeedTaskEntityMgr.updateDataFeedTask(dataFeedTask, true);
        }
        BusinessEntity businessEntity = BusinessEntity.getByName(dataFeedTask.getEntity());
        if (businessEntity.getSystemBatchStore() != null) {
            Table systemTable = dataCollectionService.getTable(customerSpace, businessEntity.getSystemBatchStore(),
                    dataCollectionService.getActiveVersion(customerSpace));
            if (systemTable == null || CollectionUtils.isEmpty(systemTable.getAttributes())) {
                return dataFeedTask.getTaskUniqueName();
            } else {
                for (Attribute attribute: systemTable.getAttributes()) {
                    if (attribute.getName().startsWith(dataFeedTask.getImportTemplate().getName())) {
                        return dataFeedTask.getImportTemplate().getName();
                    }
                }
            }
        }
        return dataFeedTask.getTaskUniqueName();
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

    @Override
    public List<SourceInfo> getSourcesBySystemPid(String customerSpace, Long systemPid) {
        return getSourcesBySystemPid(customerSpace, systemPid, 0, MAX_PAGE_SIZE);
    }

    @Override
    public List<SourceInfo> getSourcesBySystemPid(String customerSpace, Long systemPid, int pageIndex, int pageSize) {
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        return dataFeedTaskEntityMgr.getSourcesBySystemPid(systemPid, pageRequest);
    }

    @Override
    public List<SourceInfo> getSourcesByProjectId(String customerSpace, String projectId, int pageIndex, int pageSize) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(customerSpace));
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        PageRequest pageRequest = getPageRequest(pageIndex, pageSize);
        return dataFeedTaskEntityMgr.getSourcesByProjectId(projectId, customerSpace, pageRequest);
    }

    @Override
    public Long countSourcesBySystemPid(String customerSpace, Long systemPid) {
        return dataFeedTaskEntityMgr.countSourcesBySystemPid(systemPid);
    }

    @Override
    public Long countSourcesByProjectId(String customerSpace, String projectId) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(customerSpace));
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskEntityMgr.countSourcesByProjectId(projectId, customerSpace);
    }

    @Override
    public SourceInfo getSourceBySourceId(String customerSpace, String sourceId) {
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        if (dataFeed == null) {
            return null;
        }
        return dataFeedTaskEntityMgr.getSourceBySourceIdAndDataFeed(sourceId, dataFeed);
    }

    @Override
    public List<DataFeedTaskSummary> getSummaryBySourceAndDataFeed(String customerSpace, String source) {
        return dataFeedTaskEntityMgr.getSummaryBySourceAndDataFeed(source, CustomerSpace.parse(customerSpace).toString());
    }

    @Override
    public boolean existsBySourceAndFeedType(String customerSpace, String source, String feedType) {
        return dataFeedTaskEntityMgr.existsBySourceAndFeedType(source, feedType, CustomerSpace.parse(customerSpace).toString());
    }

    @Override
    public void deleteDataFeedTaskByProjectId(String customerSpace, String projectId) {
        List<SourceInfo> sourceInfos = getSourcesByProjectId(customerSpace, projectId, 0, 1000);
        sourceInfos.forEach(sourceInfo -> {
            DataFeedTask dataFeedTask = getDataFeedTask(customerSpace, sourceInfo.getPid());
            mdService.deleteImportTableAndCleanup(CustomerSpace.parse(customerSpace), dataFeedTask.getImportTemplate().getName());
            s3ImportSystemService.deleteS3ImportSystem(customerSpace, dataFeedTask.getImportSystem());
            dataFeedTaskEntityMgr.delete(dataFeedTask);
        });
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

    private PageRequest getPageRequest(int pageIndex, int pageSize) {
        Preconditions.checkState(pageIndex >= 0);
        Preconditions.checkState(pageSize > 0);
        pageSize = Math.min(pageSize, MAX_PAGE_SIZE);
        Sort sort = Sort.by(Sort.Direction.DESC, "lastUpdated");
        return PageRequest.of(pageIndex, pageSize, sort);
    }
}
