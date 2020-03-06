package com.latticeengines.apps.dcp.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
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
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.dcp.Project;
import com.latticeengines.domain.exposed.dcp.ProjectDetails;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.standardschemas.ImportWorkflowSpec;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Service("sourceService")
public class SourceServiceImpl implements SourceService {

    private static final Logger log = LoggerFactory.getLogger(SourceServiceImpl.class);

    private static final String DROP_FOLDER = "drop/";
    private static final String UPLOAD_FOLDER = "upload/";
    private static final String SOURCE_RELATIVE_PATH_PATTERN = "Source/%s/";
    private static final String RANDOM_SOURCE_ID_PATTERN = "Source_%s";
    private static final String USER_PREFIX = "user_";
    private static final String FEED_TYPE_PATTERN = "%s_%s"; // SystemName_SourceId;

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
    public Source createSource(String customerSpace, String displayName, String projectId, SimpleTemplateMetadata templateMetadata) {
        String sourceId = generateRandomSourceId(customerSpace);
        return createSource(customerSpace, displayName, projectId, sourceId, templateMetadata);
    }

    @Override
    public Source createSource(String customerSpace, String displayName, String projectId, String sourceId, SimpleTemplateMetadata templateMetadata) {
        Project project = projectService.getProjectByProjectId(customerSpace, projectId);
        if (project == null) {
            throw new RuntimeException(String.format("Cannot create source under project %s", projectId));
        }
        validateSourceId(customerSpace, sourceId);
        String relativePath = generateRelativePath(sourceId);
        Table standardTable = getTemplateFromSpec(customerSpace, project.getS3ImportSystem(),
                templateMetadata.getEntityType());
        DataFeedTask dataFeedTask = setupDataFeedTask(customerSpace, templateMetadata,
                project.getS3ImportSystem(), standardTable, relativePath, displayName, sourceId);
        Source source = convertToSource(customerSpace, dataFeedTask);
        if (StringUtils.isNotBlank(source.getFullPath())) {
            dropBoxService.createFolderUnderDropFolder(source.getFullPath());
            dropBoxService.createFolderUnderDropFolder(source.getFullPath() + DROP_FOLDER);
            dropBoxService.createFolderUnderDropFolder(source.getFullPath() + UPLOAD_FOLDER);
        }
        return source;
    }

    @Override
    public Source getSource(String customerSpace, String sourceId) {
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace, sourceId);
        return convertToSource(customerSpace, dataFeedTask);
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
            source.setFullPath(project.getRootPath() + dataFeedTask.getRelativePath());
        }
        return source;
    }

    private String generateFeedType(String systemName, String sourceId) {
        return String.format(FEED_TYPE_PATTERN, systemName, sourceId);
    }

    // TODO: Put create table code under some common place  --jhe
    private DataFeedTask setupDataFeedTask(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata,
                                           S3ImportSystem importSystem, Table standardTable,
                                           String relativePath, String displayName, String sourceId) {
        EntityType entityType = simpleTemplateMetadata.getEntityType();
        Table templateTable = generateSourceTemplate(standardTable, simpleTemplateMetadata);
        templateTable.setName(templateTable.getName() + System.currentTimeMillis());
        metadataProxy.createImportTable(customerSpace, templateTable.getName(), templateTable);
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
        dataFeedTask.setImportTemplate(templateTable);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entityType.getEntity().name());
        dataFeedTask.setFeedType(generateFeedType(importSystem.getName(), sourceId));
        dataFeedTask.setSource("File");
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedTask.setSubType(entityType.getSubType());
        dataFeedTask.setTemplateDisplayName(dataFeedTask.getFeedType());
        dataFeedTask.setImportSystem(importSystem);
        dataFeedTask.setRelativePath(relativePath);
        dataFeedTask.setSourceDisplayName(displayName);
        dataFeedTask.setSourceId(sourceId);

        dataFeedProxy.createDataFeedTask(customerSpace, dataFeedTask);

        DataFeed dataFeed = dataFeedProxy.getDataFeed(customerSpace);
        if (dataFeed.getStatus().equals(DataFeed.Status.Initing)) {
            dataFeedProxy.updateDataFeedStatus(customerSpace, DataFeed.Status.Initialized.getName());
        }

        log.debug("Successfully created DataFeedTask with FeedType {} for entity type {}",
                dataFeedTask.getFeedType(), entityType);

        return dataFeedTask;
    }

    private Table getTemplateFromSpec(String customerSpace, S3ImportSystem importSystem, EntityType entityType) {
        ImportWorkflowSpec spec;
        try {
            spec = importWorkflowSpecService.loadSpecFromS3(importSystem.getSystemType().name(),
                    entityType.getDisplayName());
        } catch (IOException e) {
            throw new RuntimeException(
                    String.format("Could not generate template for tenant %s, system type %s, and system object %s " +
                                    "because the Spec failed to load", customerSpace, importSystem.getSystemType().name(),
                            entityType.getDisplayName()), e);
        }
        return importWorkflowSpecService.tableFromRecord(null, true, spec);

    }

    private Table generateSourceTemplate(Table standardTable, SimpleTemplateMetadata simpleTemplateMetadata) {
        Preconditions.checkNotNull(standardTable);
        if (simpleTemplateMetadata == null) {
            return standardTable;
        }
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getIgnoredStandardAttributes())) {
            standardTable.getAttributes()
                    .removeIf(attribute -> !Boolean.TRUE.equals(attribute.getRequired()) &&
                            simpleTemplateMetadata.getIgnoredStandardAttributes().contains(attribute.getName()));
        }
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getStandardAttributes())) {
            configStandardAttributes(standardTable, simpleTemplateMetadata);
        }
        standardTable.getAttributes().forEach(attribute -> {
            if (StringUtils.isEmpty(attribute.getDisplayName())) {
                attribute.setDisplayName(attribute.getName());
            }
        });
        if (CollectionUtils.isNotEmpty(simpleTemplateMetadata.getCustomerAttributes())) {
            standardTable.addAttributes(generateCustomerAttributes(simpleTemplateMetadata.getCustomerAttributes()));
        }
        return standardTable;
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

    private void configStandardAttributes(Table standardTable, SimpleTemplateMetadata simpleTemplateMetadata) {
        Map<String, SimpleTemplateMetadata.SimpleTemplateAttribute> nameMapping =
                simpleTemplateMetadata.getStandardAttributes().stream()
                        .filter(simpleTemplateAttribute -> StringUtils.isNotBlank(simpleTemplateAttribute.getName()))
                        .collect(Collectors.toMap(SimpleTemplateMetadata.SimpleTemplateAttribute::getName,
                                simpleTemplateAttribute -> simpleTemplateAttribute));
        standardTable.getAttributes().forEach(attribute -> {
            if (nameMapping.containsKey(attribute.getName())) {
                attribute.setDisplayName(nameMapping.get(attribute.getName()).getDisplayName());
                if (LogicalDataType.Date.equals(attribute.getLogicalDataType())) {
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getDateFormat())) {
                        attribute.setDateFormatString(nameMapping.get(attribute.getName()).getDateFormat());
                    }
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getTimeFormat())) {
                        attribute.setTimeFormatString(nameMapping.get(attribute.getName()).getTimeFormat());
                    }
                    if (StringUtils.isNotEmpty(nameMapping.get(attribute.getName()).getTimezone())) {
                        attribute.setTimezone(nameMapping.get(attribute.getName()).getTimezone());
                    }
                }
            }
        });
    }

    private List<Attribute> generateCustomerAttributes(List<SimpleTemplateMetadata.SimpleTemplateAttribute> simpleCustomerAttrs) {
        List<Attribute> customerAttributes = new ArrayList<>();
        for (SimpleTemplateMetadata.SimpleTemplateAttribute simpleTemplateAttr : simpleCustomerAttrs) {
            Attribute attribute = new Attribute();
            attribute.setDisplayName(simpleTemplateAttr.getDisplayName());
            attribute.setNullable(Boolean.TRUE);
            attribute.setName(getCustomerAttributeName(simpleTemplateAttr.getName(), simpleTemplateAttr.getDisplayName()));
            attribute.setPhysicalDataType(simpleTemplateAttr.getPhysicalDataType().name());
            FundamentalType fundamentalType = getFundamentalType(simpleTemplateAttr.getFundamentalType(),
                    simpleTemplateAttr.getPhysicalDataType());
            attribute.setFundamentalType(fundamentalType);
            attribute.setLogicalDataType(simpleTemplateAttr.getLogicalDataType());
            if (LogicalDataType.Date.equals(attribute.getLogicalDataType())) {
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getDateFormat())) {
                    attribute.setDateFormatString(simpleTemplateAttr.getDateFormat());
                }
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getTimeFormat())) {
                    attribute.setTimeFormatString(simpleTemplateAttr.getTimeFormat());
                }
                if (StringUtils.isNotEmpty(simpleTemplateAttr.getTimezone())) {
                    attribute.setTimezone(simpleTemplateAttr.getTimezone());
                }
            }
            attribute.setTags(ModelingMetadata.INTERNAL_TAG);
            attribute.setApprovedUsageFromEnumList(getApprovedUsage(simpleTemplateAttr.getApprovedUsages()));
            customerAttributes.add(attribute);
        }
        return customerAttributes;
    }

    private String getCustomerAttributeName(String name, String displayName) {
        Preconditions.checkNotNull(displayName);
        if (StringUtils.isEmpty(name)) {
            return displayName.startsWith(USER_PREFIX) ? displayName : USER_PREFIX + displayName;
        } else {
            return name.startsWith(USER_PREFIX) ? name : USER_PREFIX + name;
        }
    }

    private FundamentalType getFundamentalType(String fundamentalType, Schema.Type dataType) {
        FundamentalType type = null;
        if (StringUtils.isNotEmpty(fundamentalType)) {
            try {
                type = FundamentalType.fromName(fundamentalType);
            } catch (IllegalArgumentException ignored) {
                // Ignore
            }
        }
        if (type == null) {
            switch (dataType) {
                case ENUM:
                    type = FundamentalType.ENUM;
                    break;
                case INT:
                case FLOAT:
                case DOUBLE:
                    type = FundamentalType.NUMERIC;
                    break;
                case LONG:
                    type = FundamentalType.DATE;
                    break;
                case BOOLEAN:
                    type = FundamentalType.BOOLEAN;
                    break;
                case STRING:
                default:
                    type = FundamentalType.ALPHA;
                    break;
            }
        }
        return type;
    }

    private List<ApprovedUsage> getApprovedUsage(List<String> approvedUsages) {
        Set<ApprovedUsage> allApprovedUsages = new HashSet<>();
        allApprovedUsages.add(ApprovedUsage.MODEL_ALLINSIGHTS);
        if (CollectionUtils.isNotEmpty(approvedUsages)) {
            for (String approvedUsage : approvedUsages) {
                ApprovedUsage approvedUsage1 = ApprovedUsage.fromName(approvedUsage);
                if (!ApprovedUsage.NONE.equals(approvedUsage1)) {
                    allApprovedUsages.add(approvedUsage1);
                }
            }
        }
        return new ArrayList<>(allApprovedUsages);
    }
}
