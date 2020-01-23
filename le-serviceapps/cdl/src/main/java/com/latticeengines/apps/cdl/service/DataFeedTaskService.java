package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
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
     * @param customerSpace Tenant identifier
     * @param entity Account / Contact
     * @return A list of template names ordered by priority.
     */
    List<String> getTemplatesBySystemPriority(String customerSpace, String entity);
}
