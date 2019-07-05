package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.proxy.exposed.cdl.AtlasExportProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;

@Component("saveAtlasExportCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaveAtlasExportCSV extends RunSparkJob<EntityExportStepConfiguration, ConvertToCSVConfig> {

    private static final Logger log = LoggerFactory.getLogger(SaveAtlasExportCSV.class);
    private static final String ISO_8601 = "yyyy-MM-dd'T'HH:mm'Z'"; // default date format

    private Map<ExportEntity, HdfsDataUnit> inputUnits;
    private Map<ExportEntity, HdfsDataUnit> outputUnits = new HashMap<>();

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

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

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Resource(name = "redshiftSegmentJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Override
    protected CustomerSpace parseCustomerSpace(EntityExportStepConfiguration stepConfiguration) {
        return stepConfiguration.getCustomerSpace();
    }

    @Override
    protected Class<ConvertToCSVJob> getJobClz() {
        return ConvertToCSVJob.class;
    }

    @Override
    protected ConvertToCSVConfig configureJob(EntityExportStepConfiguration stepConfiguration) {
        inputUnits = getMapObjectFromContext(ATLAS_EXPORT_DATA_UNIT, ExportEntity.class, HdfsDataUnit.class);
        if (MapUtils.isEmpty(inputUnits)) {
            throw new IllegalStateException("No extracted entities to be converted to csv.");
        }
        ConvertToCSVConfig config = new ConvertToCSVConfig();
        config.setInput(new ArrayList<>(inputUnits.values()));
        return config;
    }

    @Override
    protected SparkJobResult runSparkJob(LivySession session) {
        inputUnits.forEach((exportEntity, hdfsDataUnit) -> {
            ConvertToCSVConfig config = new ConvertToCSVConfig();
            config.setInput(Collections.singletonList(hdfsDataUnit));
            config.setDateAttrsFmt(getDateAttrFmtMap(exportEntity));
            config.setDisplayNames(getDisplayNameMap(exportEntity));
            config.setTimeZone("UTC");
            config.setWorkspace(getRandomWorkspace());
            config.setCompress(configuration.isCompressResult());
            log.info("Submit spark job to convert " + exportEntity + " csv.");
            SparkJobResult result = sparkJobService.runJob(session, getJobClz(), config);
            outputUnits.put(exportEntity, result.getTargets().get(0));
        });
        return null;
    }

    private Map<String, String> getDisplayNameMap(ExportEntity exportEntity) {
        List<ColumnMetadata> schema = getExportSchema(exportEntity);
        Map<String, String> displayNameMap = new HashMap<>();
        Set<String> outputCols = new HashSet<>();
        schema.forEach(cm -> {
            if (StringUtils.isNotBlank(cm.getDisplayName()) && !cm.getDisplayName().equals(cm.getAttrName())) {
                String originalDisplayName = cm.getDisplayName();
                String displayName = originalDisplayName;
                int suffix = 1;
                while (outputCols.contains(displayName.toLowerCase())) {
                    log.warn("Displayname [" + displayName + "] has already been assigned to another attr, " +
                            "cannot be assigned to [" + cm.getAttrName() + "]. Append a number to differentiate." );
                    displayName = originalDisplayName + " (" + (++suffix) + ")";
                }
                displayNameMap.put(cm.getAttrName(), displayName);
                outputCols.add(displayName.toLowerCase());
            } else {
                outputCols.add(cm.getAttrName().toLowerCase());
            }
        });
        return displayNameMap;
    }

    private Map<String, String> getDateAttrFmtMap(ExportEntity exportEntity) {
        List<ColumnMetadata> schema = getExportSchema(exportEntity);
        Map<String, String> dateFmtMap = new HashMap<>();
        schema.forEach(cm -> {
            if (LogicalDataType.Date.equals(cm.getLogicalDataType())) {
                // for now, use default format for all date attrs
                dateFmtMap.put(cm.getAttrName(), ISO_8601);
            }
        });
        return dateFmtMap;
    }

    @SuppressWarnings("unchecked")
    private List<ColumnMetadata> getExportSchema(ExportEntity exportEntity) {
        Map<BusinessEntity, List> schemaMap =
                WorkflowStaticContext.getMapObject(EXPORT_SCHEMA_MAP, BusinessEntity.class, List.class);
        if (MapUtils.isEmpty(schemaMap)) {
            throw new RuntimeException("Cannot find schema map from worklow static context.");
        }
        List<ColumnMetadata> schema = new ArrayList<>();
        if (ExportEntity.Account.equals(exportEntity)) {
            for (BusinessEntity entity: BusinessEntity.EXPORT_ENTITIES) {
                if (!BusinessEntity.Contact.equals(entity)) {
                    List<ColumnMetadata> cms = (List<ColumnMetadata>) schemaMap //
                            .getOrDefault(entity, Collections.emptyList());
                    if (CollectionUtils.isNotEmpty(cms)) {
                        if (BusinessEntity.PurchaseHistory.equals(entity)) {
                            CustomerSpace customerSpace = parseCustomerSpace(configuration);
                            DataCollection.Version version = configuration.getDataCollectionVersion();
                            String tblName = dataCollectionProxy.getTableName(customerSpace.toString(), //
                                    TableRoleInCollection.SortedProduct, version);
                            if (StringUtils.isBlank(tblName)) {
                                throw new RuntimeException("Cannot find sorted product table, " + //
                                        "while is exporting purchase history attributes.");
                            }
                            for (ColumnMetadata cm : cms) {
                                String attrName = cm.getAttrName();
                                String productId = ActivityMetricsUtils.getProductIdFromFullName(attrName);
                                String productName = getProductNameFromRedshift(tblName, productId);
                                String displayName = cm.getDisplayName();
                                if (!displayName.startsWith(productName)) {
                                    cm.setDisplayName(productName + ": " + displayName);
                                }
                            }
                        }
                        schema.addAll(cms);
                    }
                }
            }
        } else if (ExportEntity.Contact.equals(exportEntity)) {
            List<ColumnMetadata> cms = (List<ColumnMetadata>) schemaMap //
                    .getOrDefault(BusinessEntity.Contact, Collections.emptyList());
            schema.addAll(cms);
        } else {
            throw new UnsupportedOperationException("Unknown export entity " + exportEntity);
        }
        return schema;
    }

    private String getProductNameFromRedshift(String tableName, String productId) {
        String sql = String.format("SELECT %s FROM %s WHERE %s = '%s' LIMIT 1", InterfaceName.ProductName.name(), tableName, //
                InterfaceName.ProductId.name(), productId);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        return retry.execute(ctx -> redshiftJdbcTemplate.queryForObject(sql, String.class));
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        AtlasExport exportRecord = atlasExportProxy.findAtlasExportById(customerSpaceStr,
                configuration.getAtlasExportId());
        if (exportRecord == null) {
            log.error(String.format("Cannot find atlas export record for id: %s, skip save data",
                    configuration.getAtlasExportId()));
            return;
        }
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
        if (configuration.isSaveToDropfolder()) {
            saveSupportingFilesToDropfolder();
        }
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

    private void saveToDataFiles(ExportEntity exportEntity, String csvGzFilePath, AtlasExport exportRecord) {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        String targetPath = atlasExportProxy.getSystemExportPath(customerSpaceStr, false);
        String suffix = csvGzFilePath.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
        String fileName = exportEntity + "_" + exportRecord.getUuid() + suffix;
        targetPath = targetPath + fileName;
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            String finalTargetPath = targetPath;
            retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Retry=" + ctx.getRetryCount() + ") copy from " + csvGzFilePath + " to " + finalTargetPath);
                }
                copyToS3(yarnConfiguration, csvGzFilePath, finalTargetPath, systemFolderTag, systemFolderTagValue);
                return true;
            });
        } catch (Exception e) {
            log.error(String.format("Cannot save export file %s to %s", csvGzFilePath, targetPath));
        }
        atlasExportProxy.addFileToSystemPath(customerSpaceStr, exportRecord.getUuid(), fileName);
    }

    private void saveToDropfolder(ExportEntity exportEntity, String csvGzFilePath, AtlasExport exportRecord) {
        String customerSpaceStr = configuration.getCustomerSpace().toString();
        String targetPath = atlasExportProxy.getDropFolderExportPath(customerSpaceStr, exportRecord.getExportType(),
                exportRecord.getDatePrefix(), false);
        String suffix = csvGzFilePath.endsWith(".csv.gz") ? ".csv.gz" : ".csv";
        String fileName = exportEntity + suffix;
        targetPath = targetPath + fileName;

        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            String finalTargetPath = targetPath;
            retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("(Retry=" + ctx.getRetryCount() + ") copy from " + csvGzFilePath + " to " + finalTargetPath);
                }
                copyToS3(yarnConfiguration, csvGzFilePath, finalTargetPath, dropFolderTag, dropFolderTagValue);
                return true;
            });
        } catch (Exception e) {
            log.error(String.format("Cannot save export file %s to %s", csvGzFilePath, targetPath));
        }
        atlasExportProxy.addFileToDropFolder(customerSpaceStr, exportRecord.getUuid(), fileName);
    }

    private void copyToS3(Configuration configuration, String hdfsPath, String s3Path, String tag, String tagValue)
            throws IOException {
        log.info("Copy from " + hdfsPath + " to " + s3Path);
        long fileSize = HdfsUtils.getFileSize(configuration, hdfsPath);
        try (InputStream stream = HdfsUtils.getInputStream(configuration, hdfsPath)) {
            s3Service.uploadInputStreamMultiPart(s3Bucket, s3Path, stream, fileSize);
            s3Service.addTagToObject(s3Bucket, s3Path, tag, tagValue);
        }
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

    private void saveSupportingFilesToDropfolder() {

    }

}
