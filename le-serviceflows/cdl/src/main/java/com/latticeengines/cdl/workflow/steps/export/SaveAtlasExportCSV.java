package com.latticeengines.cdl.workflow.steps.export;

import static com.latticeengines.workflow.exposed.build.WorkflowStaticContext.EXPORT_SCHEMA_MAP;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;
import com.latticeengines.workflow.exposed.build.WorkflowStaticContext;


@Component("saveAtlasExportCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaveAtlasExportCSV extends RunSparkJob<EntityExportStepConfiguration, ConvertToCSVConfig, ConvertToCSVJob> {

    private static final Logger log = LoggerFactory.getLogger(SaveAtlasExportCSV.class);
    private static final String ISO_8601 = "yyyy-MM-dd'T'HH:mm'Z'"; // default date format

    private Map<ExportEntity, HdfsDataUnit> inputUnits;
    private Map<ExportEntity, HdfsDataUnit> outputUnits = new HashMap<>();

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
                while (outputCols.contains(displayName)) {
                    log.warn("Displayname [" + displayName + "] has already been assigned to another attr, " +
                            "cannot be assigned to [" + cm.getAttrName() + "]. Append a number to differentiate." );
                    displayName = originalDisplayName + " (" + (++suffix) + ")";
                }
                displayNameMap.put(cm.getAttrName(), displayName);
                outputCols.add(displayName);
            } else {
                outputCols.add(cm.getAttrName());
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
                    schema.addAll(cms);
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

    @Override
    protected void postJobExecution(SparkJobResult result) {
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
            processResultCSV(exportEntity, csvGzPath);
        }));
        if (configuration.isSaveToDropfolder()) {
            saveSupportingFilesToDropfolder();
        }
    }

    private void processResultCSV(ExportEntity exportEntity, String csvGzFilePath) {
        if (configuration.isSaveToLocal()) {
            saveToLocalForTesting(exportEntity, csvGzFilePath);
        }
        if (configuration.isSaveToDropfolder()) {
            saveToDropfolder(exportEntity, csvGzFilePath);
        } else {
            saveToDataFiles(exportEntity, csvGzFilePath);
        }
    }

    private void saveToDataFiles(ExportEntity exportEntity, String csvGzFilePath) {

    }

    private void saveToDropfolder(ExportEntity exportEntity, String csvGzFilePath) {

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
