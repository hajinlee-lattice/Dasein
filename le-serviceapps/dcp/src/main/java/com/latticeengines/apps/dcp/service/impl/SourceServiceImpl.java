package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.DataFeedTaskUtils;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Service("sourceService")
public class SourceServiceImpl implements SourceService {

    private static final Logger log = LoggerFactory.getLogger(SourceServiceImpl.class);

    private static final String DATA_FEED_TASK_SOURCE = "DCP";
    private static final String DROP_FOLDER = "drop/";
    private static final String UPLOAD_FOLDER = "Uploads/";
    private static final String SOURCE_RELATIVE_PATH_PATTERN = "Sources/%s/";
    private static final String RANDOM_SOURCE_ID_PATTERN = "Source_%s";
    private static final String TEMPLATE_NAME = "%s_Template";
    private static final String FEED_TYPE_PATTERN = "%s_%s"; // SystemName_SourceId;
    private static final String FULL_PATH_PATTERN = "%s/%s/%s"; // {bucket}/{dropfolder}/{project+source path}

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ProjectService projectService;

    @Inject
    private ImportWorkflowSpecService importWorkflowSpecService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxService dropBoxService;

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId,
                               String importFile, FieldDefinitionsRecord fieldDefinitionsRecord) {
        String sourceId = generateRandomSourceId(customerSpace);
        return createSource(customerSpace, displayName, projectId, sourceId, importFile, fieldDefinitionsRecord);
    }

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                               String importFile, FieldDefinitionsRecord fieldDefinitionsRecord) {
        Project project = projectService.getProjectByProjectId(customerSpace, projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Cannot create source under project %s", projectId));
        }
        validateSourceId(customerSpace, sourceId);
        String relativePath = generateRelativePath(sourceId);

        Table templateTable = getTableFromRecord(importFile, customerSpace, sourceId, fieldDefinitionsRecord);

        DataFeedTask dataFeedTask = setupDataFeedTask(customerSpace, project.getS3ImportSystem(), templateTable,
                EntityType.fromDisplayNameToEntityType(fieldDefinitionsRecord.getSystemObject()), relativePath,
                displayName, sourceId);
        Source source = convertToSource(customerSpace, dataFeedTask);
        if (StringUtils.isNotBlank(source.getSourceFullPath())) {
            String relativePathUnderDropfolder = source.getRelativePathUnderDropfolder();
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder);
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder + DROP_FOLDER);
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder + UPLOAD_FOLDER);
        }
        return source;
    }

    @Override
    public Source updateSource(String customerSpace, String displayName, String sourceId, String importFile,
                               FieldDefinitionsRecord fieldDefinitionsRecord) {
        Table newTable = getTableFromRecord(importFile, customerSpace, sourceId, fieldDefinitionsRecord);

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId);
        Preconditions.checkNotNull(dataFeedTask, String.format("Can't retrieve data feed task for source %s",
                sourceId));

        log.info("Found existing DataFeedTask template: {}", dataFeedTask.getTemplateDisplayName());
        if (StringUtils.isNotBlank(displayName)) {
            dataFeedTask.setSourceDisplayName(displayName);
        }
        Table existingTable = dataFeedTask.getImportTemplate();
        if (!TableUtils.compareMetadataTables(existingTable, newTable)) {
            Table mergedTable = TableUtils.mergeMetadataTables(existingTable, newTable);
            dataFeedTask.setImportTemplate(mergedTable);
        }
        dataFeedProxy.updateDataFeedTask(customerSpace, dataFeedTask);
        return convertToSource(customerSpace, dataFeedTask);
    }

    @Override
    public Source getSource(String customerSpace, String sourceId) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId);
        return convertToSource(customerSpace, dataFeedTask);
    }

    @Override
    public Boolean deleteSource(String customerSpace, String sourceId) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId);
        if (dataFeedTask == null || dataFeedTask.getPid() == null) {
            throw new RuntimeException(String.format("Cannot find source %s for delete!", sourceId));
        }
        dataFeedProxy.setDataFeedTaskDeletedStatus(customerSpace, dataFeedTask.getPid(), Boolean.TRUE);
        return true;
    }

    @Override
    public Boolean pauseSource(String customerSpace, String sourceId) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId);
        if (dataFeedTask == null || dataFeedTask.getPid() == null) {
            throw new RuntimeException(String.format("Cannot find source %s for update!", sourceId));
        }
        dataFeedProxy.setDataFeedTaskS3ImportStatus(customerSpace, dataFeedTask.getPid(), DataFeedTask.S3ImportStatus.Pause);
        return true;
    }

    @Override
    public List<Source> getSourceList(String customerSpace, String projectId) {
        ProjectDetails projectDetail = projectService.getProjectDetailByProjectId(customerSpace, projectId);
        return projectDetail.getSources();
    }

    private void validateSourceId(String customerSpace, String sourceId) {
        if (StringUtils.isBlank(sourceId)) {
            throw new RuntimeException("Cannot create DCP source with blank sourceId!");
        }
        if (!sourceId.matches("[A-Za-z0-9_]*")) {
            throw new RuntimeException("Invalid characters in source id, only accept digits, alphabet & underline.");
        }
        if (dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId) != null) {
            throw new RuntimeException(String.format("SourceId %s already exists.", sourceId));
        }
    }

    @Override
    public Source convertToSource(String customerSpace, DataFeedTask dataFeedTask) {
        if (dataFeedTask == null) {
            return null;
        }
        Source source = new Source();

        source.setImportStatus(dataFeedTask.getS3ImportStatus());
        source.setSourceId(dataFeedTask.getSourceId());
        source.setSourceDisplayName(dataFeedTask.getSourceDisplayName());
        source.setRelativePath(dataFeedTask.getRelativePath());
        if (StringUtils.isNotEmpty(dataFeedTask.getImportSystemName())) {
            S3ImportSystem s3ImportSystem = cdlProxy.getS3ImportSystem(customerSpace,
                    dataFeedTask.getImportSystemName());
            Project project = projectService.getProjectByImportSystem(customerSpace, s3ImportSystem);
            source.setSourceFullPath(String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(),
                    dropBoxService.getDropBoxPrefix(), project.getRootPath() + dataFeedTask.getRelativePath()));
            source.setDropFullPath(source.getSourceFullPath() + DROP_FOLDER);
        }
        return source;
    }

    private String generateFeedType(String systemName, String sourceId) {
        return String.format(FEED_TYPE_PATTERN, systemName, sourceId);
    }

    private DataFeedTask setupDataFeedTask(String customerSpace, S3ImportSystem importSystem, Table templateTable,
                                           EntityType entityType, String relativePath, String displayName,
                                           String sourceId) {
        templateTable.setName(templateTable.getName() + System.currentTimeMillis());
        metadataProxy.createImportTable(customerSpace, templateTable.getName(), templateTable);

        DataFeedTask dataFeedTask = DataFeedTaskUtils.generateDataFeedTask(
                generateFeedType(importSystem.getName(), sourceId),
                DATA_FEED_TASK_SOURCE, importSystem, templateTable, entityType, relativePath, displayName, sourceId);

        dataFeedProxy.createDataFeedTask(customerSpace, dataFeedTask);

        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace);
        if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
            dataFeedProxy.updateDataFeedStatus(customerSpace, DataFeed.Status.Initialized.getName());
        }

        log.debug("Successfully created DataFeedTask with FeedType {} for entity type {}",
                dataFeedTask.getFeedType(), entityType);

        return dataFeedTask;
    }

    private String generateRandomSourceId(String customerSpace) {
        String randomSourceId = String.format(RANDOM_SOURCE_ID_PATTERN,
                RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        while (dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, randomSourceId) != null) {
            randomSourceId = String.format(RANDOM_SOURCE_ID_PATTERN,
                    RandomStringUtils.randomAlphanumeric(8).toLowerCase());
        }
        return randomSourceId;
    }

    private String generateRelativePath(String sourceId) {
        return String.format(SOURCE_RELATIVE_PATH_PATTERN, sourceId);
    }

    private Table getTableFromRecord(String importFile, String customerSpace, String sourceId,
                                    FieldDefinitionsRecord fieldDefinitionsRecord) {
        if (StringUtils.isNotBlank(importFile)) {
            SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, importFile);
            Preconditions.checkNotNull(sourceFile, String.format("Could not locate source file with name %s",
                    importFile));
            String newTableName = "SourceFile_" + sourceFile.getName().replace(".", "_");
            Table newTable = importWorkflowSpecService.tableFromRecord(newTableName, false,
                    fieldDefinitionsRecord);

            // Delete old table associated with the source file from the database if it exists.
            if (StringUtils.isNotBlank(sourceFile.getTableName())) {
                metadataProxy.deleteTable(customerSpace, sourceFile.getTableName());
            }

            // Associate the new table with the source file and add new table to the database.
            metadataProxy.createTable(customerSpace, newTable.getName(), newTable);
            sourceFile.setTableName(newTable.getName());
            sourceFileProxy.update(customerSpace, sourceFile);
            return newTable;
        } else {
            return importWorkflowSpecService.tableFromRecord(String.format(TEMPLATE_NAME, sourceId), false,
                    fieldDefinitionsRecord);
        }
    }
}
