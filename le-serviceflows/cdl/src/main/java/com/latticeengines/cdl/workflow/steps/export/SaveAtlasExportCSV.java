package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.ATLAS_EXPORT;
import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.domain.exposed.util.ExportUtils;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.serviceflows.workflow.dataflow.BaseSparkStep;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("saveAtlasExportCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaveAtlasExportCSV extends BaseSparkStep<EntityExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(SaveAtlasExportCSV.class);
    private static final String ISO_8601 = ConvertToCSVConfig.ISO_8601; // default date format

    private Map<ExportEntity, HdfsDataUnit> inputUnits;
    private Map<ExportEntity, HdfsDataUnit> outputUnits = new HashMap<>();

    @Value("${cdl.atlas.export.dropfolder.tag}")
    private String dropFolderTag;

    @Value("${cdl.atlas.export.dropfolder.tag.value}")
    private String dropFolderTagValue;

    @Value("${cdl.atlas.export.systemfolder.tag}")
    private String systemFolderTag;

    @Value("${cdl.atlas.export.systemfolder.tag.value}")
    private String systemFolderTagValue;

    @Inject
    private AtlasExportProxy atlasExportProxy;

    private Map<BusinessEntity, Boolean> changeDisplayNameMap = new HashMap<>();

    @Override
    protected CustomerSpace parseCustomerSpace(EntityExportStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    private boolean getAddExportTimestamp(AtlasExport exportRecord) {
        if (exportRecord.getExportConfig() != null) {
            return exportRecord.getExportConfig().getAddExportTimestamp();
        } else {
            return false;
        }
    }

    @Override
    public void execute() {
        AtlasExport exportRecord = WorkflowStaticContext.getObject(ATLAS_EXPORT, AtlasExport.class);
        if (exportRecord == null) {
            log.error(String.format("Cannot find atlas export record for id: %s, skip save data",
                    configuration.getAtlasExportId()));
            return;
        }
        boolean addExportTimestamp = getAddExportTimestamp(exportRecord);
        customerSpace = parseCustomerSpace(configuration);
        inputUnits = getMapObjectFromContext(ATLAS_EXPORT_DATA_UNIT, ExportEntity.class, HdfsDataUnit.class);
        inputUnits.forEach((exportEntity, hdfsDataUnit) -> {
            ConvertToCSVConfig config = new ConvertToCSVConfig();
            config.setInput(Collections.singletonList(hdfsDataUnit));
            config.setDateAttrsFmt(getDateAttrFmtMap(exportEntity, addExportTimestamp));
            config.setDisplayNames(getDisplayNameMap(exportEntity));
            config.setTimeZone("UTC");
            config.setWorkspace(getRandomWorkspace());
            config.setCompress(configuration.isCompressResult());
            if (addExportTimestamp) {
                config.setExportTimeAttr(InterfaceName.LatticeExportTime.name());
            }
            log.info("Submit spark job to convert " + exportEntity + " csv.");
            SparkJobResult result = runSparkJob(ConvertToCSVJob.class, config);
            outputUnits.put(exportEntity, result.getTargets().get(0));
        });
        outputUnits.forEach(((exportEntity, hdfsDataUnit) -> {
            String outputDir = hdfsDataUnit.getPath();
            String csvGzPath;
            try {
                List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, //
                        (HdfsUtils.HdfsFilenameFilter) filename -> //
                                filename.endsWith(".csv.gz") || filename.endsWith(".csv"));
                csvGzPath = files.get(0);
            } catch (IOException e) {
                throw new RuntimeException("Failed to read " + outputDir);
            }
            processResultCSV(exportEntity, csvGzPath, exportRecord);
        }));
    }

    private Map<String, String> getDisplayNameMap(ExportEntity exportEntity) {
        List<ColumnMetadata> schema = getExportSchema(exportEntity);
        return ExportUtils.getDisplayNameMap(exportEntity, schema);
    }

    private Map<String, String> getDateAttrFmtMap(ExportEntity exportEntity, boolean addExportTimestamp) {
        List<ColumnMetadata> schema = getExportSchema(exportEntity);
        Map<String, String> dateFmtMap = new HashMap<>();
        schema.forEach(cm -> {
            if (LogicalDataType.Date.equals(cm.getLogicalDataType())) {
                // need to rename contact column name
                if (ExportEntity.AccountContact.equals(exportEntity) && //
                        BusinessEntity.Contact.equals(BusinessEntity.getCentralEntity(cm.getEntity()))) {
                    // for now, use default format for all date attrs
                    dateFmtMap.put(ExportUtils.CONTACT_ATTR_PREFIX + cm.getAttrName(), ISO_8601);
                } else {
                    // for now, use default format for all date attrs
                    dateFmtMap.put(cm.getAttrName(), ISO_8601);
                }
            }
        });
        if (addExportTimestamp) {
            dateFmtMap.put(InterfaceName.LatticeExportTime.name(), ISO_8601);
        }
        return dateFmtMap;
    }

    @SuppressWarnings("unchecked")
    private void setAccountSchema(Map<BusinessEntity, List> schemaMap, List<ColumnMetadata> schema) {
        for (BusinessEntity entity : BusinessEntity.EXPORT_ACCOUNT_ENTITIES) {
            changeDisplayNameIfNeeded(schemaMap, entity, schema);
        }
    }

    private void changeDisplayNameIfNeeded(Map<BusinessEntity, List> schemaMap, BusinessEntity entity, List<ColumnMetadata> schema) {
        List<ColumnMetadata> cms = (List<ColumnMetadata>) schemaMap.getOrDefault(entity, Collections.emptyList());
        if (CollectionUtils.isNotEmpty(cms)) {
            changeDisplayNameMap.putIfAbsent(entity, true);
            if (BusinessEntity.ENTITIES_WITH_HIRERARCHICAL_DISPLAY_NAME.contains(entity) && changeDisplayNameMap.get(entity)) {
                log.info("May need to change display name of some attributes which have subcategory for entity {}.", entity.name());
                for (ColumnMetadata cm : cms) {
                    String displayName = cm.getDisplayName();
                    String subCategory = cm.getSubcategory();
                    if (StringUtils.isNotBlank(displayName) && StringUtils.isNotBlank(subCategory)
                            && !Category.SUB_CAT_OTHER.equalsIgnoreCase(subCategory)) {
                        cm.setDisplayName(subCategory + ": " + displayName);
                    }
                }
                changeDisplayNameMap.put(entity, false);
            }
            schema.addAll(cms);
        }
    }

    @SuppressWarnings("unchecked")
    private void setContactSchema(Map<BusinessEntity, List> schemaMap, List<ColumnMetadata> schema) {
        for (BusinessEntity entity : BusinessEntity.EXPORT_CONTACT_ENTITIES) {
            changeDisplayNameIfNeeded(schemaMap, entity, schema);
        }
    }

    @SuppressWarnings("unchecked")
    private List<ColumnMetadata> getExportSchema(ExportEntity exportEntity) {
        Map<BusinessEntity, List> schemaMap =
                WorkflowStaticContext.getMapObject(EXPORT_SCHEMA_MAP, BusinessEntity.class, List.class);
        List<ColumnMetadata> schema = new ArrayList<>();
        if (ExportEntity.Account.equals(exportEntity)) {
            setAccountSchema(schemaMap, schema);
        } else if (ExportEntity.Contact.equals(exportEntity)) {
            setContactSchema(schemaMap, schema);
        } else if (ExportEntity.AccountContact.equals(exportEntity)) {
            setAccountSchema(schemaMap, schema);
            setContactSchema(schemaMap, schema);
        } else {
            throw new UnsupportedOperationException("Unknown export entity " + exportEntity);
        }
        schema.sort((cm1, cm2) -> {
            if (cm1.getCategory() == null) {
                return -1;
            }
            if (cm2.getCategory() == null) {
                return 1;
            }
            return cm1.getCategory().getOrder().compareTo(cm2.getCategory().getOrder());
        });
        return schema;
    }

    private void processResultCSV(ExportEntity exportEntity, String csvGzFilePath, AtlasExport exportRecord) {
        if (configuration.isSaveToLocal()) {
            saveToLocalForTesting(exportEntity, csvGzFilePath);
        }
        if (configuration.isSaveToDropfolder()) {
            saveToDropfolder(exportEntity, csvGzFilePath, exportRecord);
        } else {
            saveToDataFiles(exportEntity, csvGzFilePath, exportRecord);
        }
    }

    private List<String> getDeletePath() {
        List<String> files = getListObjectFromContext(ATLAS_EXPORT_DELETE_PATH, String.class);
        files.addAll(outputUnits.values().stream().map(hdfsDataUnit -> hdfsDataUnit.getPath().substring(0,
                hdfsDataUnit.getPath().lastIndexOf("/"))).collect(Collectors.toList()));
        return files;
    }

    private void saveToDataFiles(ExportEntity exportEntity, String csvGzFilePath, AtlasExport exportRecord) {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        String targetPath = atlasExportProxy.getSystemExportPath(customerSpaceStr, false);
        String suffix = csvGzFilePath.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
        String fileName = exportEntity + "_" + getExportName(exportRecord) + suffix;
        copyToS3(targetPath, fileName, csvGzFilePath, false);
        List<String> deletePathList = getDeletePath();
        atlasExportProxy.addFileToSystemPath(customerSpaceStr, exportRecord.getUuid(), fileName, deletePathList);
        cleanUpTempPath(deletePathList);
    }

    private String getExportName(AtlasExport atlasExport) {
        if (StringUtils.isEmpty(atlasExport.getSegmentName())) {
            return atlasExport.getUuid();
        }
        return ExportUtils.getReplacedName(atlasExport.getSegmentName()) + "_" + atlasExport.getUuid();
    }

    private void cleanUpTempPath(List<String> deletePathList) {
        if (CollectionUtils.isNotEmpty(deletePathList)) {
            for (String path : deletePathList) {
                try {
                    if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    }
                } catch (IOException e) {
                    log.error(String.format("Could not delete temp export path %s", e.getMessage()));
                }
            }
        }
    }

    private void copyToS3(String targetPath, String fileName, String csvGzFilePath, boolean dropFolderFlag) {
        targetPath = targetPath + fileName;
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            String finalTargetPath = targetPath;
            retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Retry=" + ctx.getRetryCount() + ") copy from " + csvGzFilePath + " to " + finalTargetPath
                            + ". Previous error: ", ctx.getLastThrowable());
                }
                if (dropFolderFlag) {
                    copyToS3(csvGzFilePath, finalTargetPath, dropFolderTag, dropFolderTagValue);
                } else {
                    copyToS3(csvGzFilePath, finalTargetPath, systemFolderTag, systemFolderTagValue);
                }
                return true;
            });
        } catch (Exception e) {
            AtlasExport atlasExport = WorkflowStaticContext.getObject(ATLAS_EXPORT, AtlasExport.class);
            if (atlasExport == null) {
                throw new RuntimeException("Cannot find atlasExport in context");
            }
            atlasExportProxy.updateAtlasExportStatus(configuration.getCustomerSpace().toString(), atlasExport.getUuid(),
                    MetadataSegmentExport.Status.FAILED);
            log.error(String.format("Cannot save export file %s to %s", csvGzFilePath, targetPath));
        }
    }

    private void saveToDropfolder(ExportEntity exportEntity, String csvGzFilePath, AtlasExport exportRecord) {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        String targetPath = atlasExportProxy.getDropFolderExportPath(customerSpaceStr, exportRecord.getExportType(),
                exportRecord.getDatePrefix(), false);
        String suffix = csvGzFilePath.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
        String fileName = exportEntity + "_" + exportRecord.getUuid() + suffix;
        copyToS3(targetPath, fileName, csvGzFilePath, true);
        List<String> deletePathList = getDeletePath();
        atlasExportProxy.addFileToDropFolder(customerSpaceStr, exportRecord.getUuid(), fileName, deletePathList);
        cleanUpTempPath(deletePathList);
    }

    private void saveToLocalForTesting(ExportEntity exportEntity, String csvGzFilePath) {
        String suffix = csvGzFilePath.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
        String tgtPath = "/tmp/ExtractEntityTest/" + exportEntity + suffix;
        File tgtFile = new File(tgtPath);
        FileUtils.deleteQuietly(tgtFile);
        try {
            log.info("Copying " + csvGzFilePath + " to " + tgtPath);
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, csvGzFilePath, tgtPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download hdfs file " + csvGzFilePath, e);
        }
    }

}
