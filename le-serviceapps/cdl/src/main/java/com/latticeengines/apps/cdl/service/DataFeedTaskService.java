package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.SourceInfo;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public interface DataFeedTaskService {

    void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask);

    void createOrUpdateDataFeedTask(String customerSpace, String source, String dataFeedType, String entity,
                                    String tableName);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity);

    DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType);

    DataFeedTask getDataFeedTask(String customerSpace, Long taskId);

    DataFeedTask getDataFeedTask(String customerSpace, String uniqueId);

    List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity);

    List<DataFeedTask> getDataFeedTaskWithSameEntityExcludeOne(String customerSpace, String entity,
                                                               String excludeSource, String excludeFeedType);

    List<DataFeedTask> getDataFeedTaskByUniqueIds(String customerSpace, List<String> uniqueIds);

    void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask, boolean updateTaskOnly);

    void updateS3ImportStatus(String customerSpace, String source, String dataFeedType, DataFeedTask.S3ImportStatus status);

    void updateS3ImportStatus(String customerSpace, String uniqueId, DataFeedTask.S3ImportStatus status);

    List<String> registerExtract(String customerSpace, String taskUniqueId, String tableName, Extract extract);

    List<String> registerExtracts(String customerSpace, String taskUniqueId, String tableName, List<Extract> extracts);

    void addTableToQueue(String customerSpace, String taskUniqueId, String tableName);

    void addTablesToQueue(String customerSpace, String taskUniqueId, List<String> tableNames);

    List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType, String entity);

    void resetImport(String customerSpace, DataFeedTask datafeedTask);

    List<Table> getTemplateTables(String customerSpace, String entity);

    S3ImportSystem getImportSystemByTaskId(String customerSpace, String taskUniqueId);

    /**
     *
     * @param entity Account / Contact
     * @param highestFirst: True - template with highest priority will be the first item. False - reverse
     * @return A list of template names ordered by priority.
     */
    List<String> getTemplatesBySystemPriority(String customerSpace, String entity, boolean highestFirst);

    /**
     *
     * @param taskUniqueId DataFeedTask.uniqueId
     * @return template name: could be DataFeedTask.taskUniqueName or template table name (if already used in
     * SystemStore)
     */
    String getTemplateName(String customerSpace, String taskUniqueId);

    /**
     *
     * @return templateName -> systemName Map
     */
    Map<String, String> getTemplateToSystemMap(String customerSpace);

    /**
     *
     * @return templateName -> systemType Map
     */
    Map<String, S3ImportSystem.SystemType> getTemplateToSystemTypeMap(String customerSpace);

    /**
     *
     * @return templateName -> S3ImportSystemObj
     */
    Map<String, S3ImportSystem> getTemplateToSystemObjectMap(String customerSpace);

    /**
     * \
     * @return templateName -> DataFeedTask
     */
    Map<String, DataFeedTask> getTemplateToDataFeedTaskMap(String customerSpace);

    DataFeedTask getDataFeedTaskBySource(String customerSpace, String sourceId);

    void setDataFeedTaskDelete(String customerSpace, Long pid, Boolean deleted);

    void setDataFeedTaskS3ImportStatus(String customerSpace, Long pid, DataFeedTask.S3ImportStatus status);

    List<SourceInfo> getSourcesBySystemPid(String customerSpace, Long systemPid);

    SourceInfo getSourceBySourceId(String customerSpace, String sourceId);
}
