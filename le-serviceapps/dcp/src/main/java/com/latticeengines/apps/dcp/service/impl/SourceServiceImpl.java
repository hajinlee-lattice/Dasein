package com.latticeengines.apps.dcp.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.service.ImportWorkflowSpecService;
import com.latticeengines.apps.dcp.service.DataReportService;
import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.apps.dcp.service.ProjectService;
import com.latticeengines.apps.dcp.service.ProjectSystemLinkService;
import com.latticeengines.apps.dcp.service.SourceService;
import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.dcp.DataReport;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.dcp.ProjectInfo;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceInfo;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
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
    private static final String SYSTEM_NAME_PATTERN = "SourceSystem_%s";
    private static final String RANDOM_SOURCE_ID_PATTERN = "Source_%s";
    private static final String TEMPLATE_NAME = "%s_Template";
    private static final String FEED_TYPE_PATTERN = "%s_%s"; // SystemName_SourceId;
    private static final String FULL_PATH_PATTERN = "%s/%s/%s"; // {bucket}/{dropfolder}/{project+source path}
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_RETRY = 3;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ProjectService projectService;

    @Inject
    private UploadService uploadService;

    @Inject
    private ProjectSystemLinkService projectSystemLinkService;

    @Inject
    private ImportWorkflowSpecService importWorkflowSpecService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private DataReportService dataReportService;

    @Inject
    private MatchRuleService matchRuleService;

    @Inject
    private CDLProxy cdlProxy;

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId,
                               FieldDefinitionsRecord fieldDefinitionsRecord) {
        String sourceId = generateRandomSourceId(customerSpace);
        return createSource(customerSpace, displayName, projectId, sourceId, fieldDefinitionsRecord);
    }

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                               FieldDefinitionsRecord fieldDefinitionsRecord) {
        return createSource(customerSpace, displayName, projectId, sourceId, null, fieldDefinitionsRecord);
    }

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId, String sourceId,
                               String fileImportId, FieldDefinitionsRecord fieldDefinitionsRecord) {
        ProjectInfo projectInfo = projectService.getProjectInfoByProjectId(customerSpace, projectId);
        if (projectInfo == null) {
            throw new RuntimeException(String.format("Cannot create source under project %s", projectId));
        }
        if (StringUtils.isBlank(sourceId)) {
            sourceId = generateRandomSourceId(customerSpace);
        }
        validateSourceId(customerSpace, sourceId);
        S3ImportSystem importSystem = createSourceSystem(customerSpace, displayName, sourceId);
        String relativePath = generateRelativePath(sourceId);

        Table templateTable = getTableFromRecord(fileImportId, customerSpace, sourceId, fieldDefinitionsRecord);

        DataFeedTask dataFeedTask = setupDataFeedTask(customerSpace, importSystem, templateTable,
                EntityType.fromDisplayNameToEntityType(fieldDefinitionsRecord.getSystemObject()), relativePath,
                displayName, sourceId);
        Source source = getSourceFromDataFeedTask(projectInfo, dataFeedTask);
        if (StringUtils.isNotBlank(source.getSourceFullPath())) {
            String relativePathUnderDropfolder = source.getRelativePathUnderDropfolder();
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder);
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder + DROP_FOLDER);
            dropBoxService.createFolderUnderDropFolder(relativePathUnderDropfolder + UPLOAD_FOLDER);
        }

        // link project & system
        projectSystemLinkService.createLink(customerSpace, projectId, importSystem);

        // Generate default base rule for new source
        MatchRule defaultRule = new MatchRule();
        defaultRule.setSourceId(source.getSourceId());
        defaultRule.setDisplayName("Base Rule");
        defaultRule.setRuleType(MatchRuleRecord.RuleType.BASE_RULE);
        defaultRule.setState(MatchRuleRecord.State.ACTIVE);
        defaultRule.setAcceptCriterion(DplusMatchRule.getDefaultAcceptCriterion());
        if (projectInfo.getPurposeOfUse() != null) {
            defaultRule.setDomain(projectInfo.getPurposeOfUse().getDomain());
            defaultRule.setRecordType(projectInfo.getPurposeOfUse().getRecordType());
        }
        matchRuleService.createMatchRule(customerSpace, defaultRule);

        return source;
    }

    @Override
    public Source updateSource(String customerSpace, String displayName, String sourceId, String fileImportId,
                               FieldDefinitionsRecord fieldDefinitionsRecord) {
        Table newTable = getTableFromRecord(fileImportId, customerSpace, sourceId, fieldDefinitionsRecord);
        ProjectInfo projectInfo = projectService.getProjectBySourceId(customerSpace, sourceId);
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
        return getSourceFromDataFeedTask(projectInfo, dataFeedTask);
    }

    @Override
    public Source getSource(String customerSpace, String sourceId) {
        ProjectInfo projectInfo = projectService.getProjectBySourceId(customerSpace, sourceId);
        SourceInfo sourceInfo = dataFeedProxy.getSourceBySourceId(customerSpace, sourceId);
        DataReport.BasicStats basicStats = dataReportService.getDataReportBasicStats(customerSpace,
                DataReportRecord.Level.Source, sourceId);
        return getSourceFromSourceInfo(projectInfo, sourceInfo, basicStats);
    }

    @Override
    public Boolean deleteSource(String customerSpace, String sourceId) {
        SourceInfo sourceInfo = dataFeedProxy.getSourceBySourceId(customerSpace, sourceId);
        if (sourceInfo == null || sourceInfo.getPid() == null) {
            throw new RuntimeException(String.format("Cannot find source %s for delete!", sourceId));
        }
        dataFeedProxy.setDataFeedTaskDeletedStatus(customerSpace, sourceInfo.getPid(), Boolean.TRUE);
        return true;
    }

    @Override
    public Boolean hardDeleteSourceUnderProject(String customerSpace, String projectId) {
        List<SourceInfo> sourceInfos = dataFeedProxy.getSourcesByProjectId(customerSpace, projectId, 0, 100);
        dataFeedProxy.deleteDataFeedTaskUnderProjectId(customerSpace, projectId);
        sourceInfos.forEach(sourceInfo -> {
            uploadService.hardDeleteUploadUnderSource(customerSpace, sourceInfo.getSourceId());
            matchRuleService.hardDeleteMatchRuleBySourceId(customerSpace, sourceInfo.getSourceId());
        });

        return true;
    }

    @Override
    public Boolean pauseSource(String customerSpace, String sourceId) {
        SourceInfo sourceInfo = dataFeedProxy.getSourceBySourceId(customerSpace, sourceId);
        if (sourceInfo == null || sourceInfo.getPid() == null) {
            throw new RuntimeException(String.format("Cannot find source %s to pause!", sourceId));
        }
        dataFeedProxy.setDataFeedTaskS3ImportStatus(customerSpace, sourceInfo.getPid(), DataFeedTask.S3ImportStatus.Pause);
        return true;
    }

    @Override
    public List<Source> getSourceList(String customerSpace, String projectId) {
        return getSourceList(customerSpace, projectId, 0, DEFAULT_PAGE_SIZE, null);
    }

    @Override
    public List<Source> getSourceList(String customerSpace, String projectId, int pageIndex, int pageSize, List<String> teamIds) {
        ProjectInfo projectInfo = projectService.getProjectInfoByProjectId(customerSpace, projectId);
        if (projectInfo != null && (projectInfo.getTeamId() == null || (teamIds == null || teamIds.contains(projectInfo.getTeamId())))) {
            List<SourceInfo> sourceInfoList = dataFeedProxy.getSourcesByProjectId(customerSpace,
                    projectInfo.getProjectId(), pageIndex, pageSize);
            if (CollectionUtils.isEmpty(sourceInfoList)) {
                return Collections.emptyList();
            }
            Map<String, DataReport.BasicStats> sourceBasicStatsMap =
                    dataReportService.getDataReportBasicStatsByParent(customerSpace, DataReportRecord.Level.Project, projectId);
            return sourceInfoList.stream()
                    .map(sourceInfo -> getSourceFromSourceInfo(projectInfo, sourceInfo, sourceBasicStatsMap.get(sourceInfo.getSourceId())))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public long getSourceCount(String customerSpace, Long systemPid) {
        Long count = dataFeedProxy.countSourcesBySystemPid(customerSpace, systemPid);
        return count == null ? 0L : count;
    }

    @Override
    public long getSourceCount(String customerSpace, String projectId) {
        Long count = dataFeedProxy.countSourcesByProjectId(customerSpace, projectId);
        return count == null ? 0L : count;
    }

    private Source getSourceFromSourceInfo(ProjectInfo projectInfo, SourceInfo sourceInfo,
                                           DataReport.BasicStats sourceBasicStats) {
        Source source = new Source();
        source.setImportStatus(sourceInfo.getImportStatus());
        source.setSourceId(sourceInfo.getSourceId());
        source.setSourceDisplayName(sourceInfo.getSourceDisplayName());
        source.setRelativePath(sourceInfo.getRelativePath());
        source.setSourceFullPath(String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(),
                dropBoxService.getDropBoxPrefix(), projectInfo.getRootPath() + sourceInfo.getRelativePath()));
        source.setBasicStats(sourceBasicStats);
        source.setDropFullPath(source.getSourceFullPath() + DROP_FOLDER);
        return source;
    }

    private Source getSourceFromDataFeedTask(ProjectInfo projectInfo, DataFeedTask dataFeedTask) {
        Source source = new Source();
        source.setImportStatus(dataFeedTask.getS3ImportStatus());
        source.setSourceId(dataFeedTask.getSourceId());
        source.setSourceDisplayName(dataFeedTask.getSourceDisplayName());
        source.setRelativePath(dataFeedTask.getRelativePath());
        source.setSourceFullPath(String.format(FULL_PATH_PATTERN, dropBoxService.getDropBoxBucket(),
                dropBoxService.getDropBoxPrefix(), projectInfo.getRootPath() + dataFeedTask.getRelativePath()));
        source.setDropFullPath(source.getSourceFullPath() + DROP_FOLDER);
        return source;
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

    private Table getTableFromRecord(String fileImportId, String customerSpace, String sourceId,
                                    FieldDefinitionsRecord fieldDefinitionsRecord) {
        if (StringUtils.isNotBlank(fileImportId)) {
            SourceFile sourceFile = sourceFileProxy.findByName(customerSpace, fileImportId);
            Preconditions.checkNotNull(sourceFile, String.format("Could not locate source file with name %s",
                    fileImportId));
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

    @Override
    public Boolean reactivateSource(String customerSpace, String sourceId) {
        SourceInfo sourceInfo = dataFeedProxy.getSourceBySourceId(customerSpace, sourceId);
        if (sourceInfo == null || sourceInfo.getPid() == null) {
            throw new RuntimeException(String.format("Cannot find source %s to reactivate!", sourceId));
        }
        if(DataFeedTask.S3ImportStatus.Pause.equals(sourceInfo.getImportStatus())) {
            dataFeedProxy.setDataFeedTaskS3ImportStatus(customerSpace, sourceInfo.getPid(), DataFeedTask.S3ImportStatus.Active);
        }
        return true;
    }

    private S3ImportSystem createSourceSystem(String customerSpace, String displayName, String sourceId) {
        S3ImportSystem system = new S3ImportSystem();
        system.setTenant(MultiTenantContext.getTenant());
        String systemName = String.format(SYSTEM_NAME_PATTERN, sourceId);
        system.setName(systemName);
        system.setDisplayName(displayName);
        system.setSystemType(S3ImportSystem.SystemType.DCP);
        cdlProxy.createS3ImportSystem(customerSpace, system);
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(MAX_RETRY,
                Collections.singleton(IllegalArgumentException.class), null);
        system = retryTemplate.execute(context -> {
            S3ImportSystem createdSystem = cdlProxy.getS3ImportSystem(customerSpace, systemName);
            if (createdSystem == null) {
                throw new IllegalArgumentException("Cannot get importSystem: " + systemName);
            }
            return createdSystem;
        });
        if (system == null) {
            throw new RuntimeException("Cannot create DCP Project due to ImportSystem creation error!");
        }
        return system;
    }
}
