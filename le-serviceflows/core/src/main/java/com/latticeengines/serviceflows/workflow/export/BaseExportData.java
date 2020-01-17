package com.latticeengines.serviceflows.workflow.export;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.MergeCSVConfig;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.serviceflows.workflow.dataflow.LivySessionManager;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.job.cdl.MergeCSVJob;
import com.latticeengines.spark.exposed.service.SparkJobService;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseExportData<T extends ExportStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseExportData.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private EaiProxy eaiProxy;

    @Inject
    private SparkJobService sparkJobService;

    @Inject
    private LivySessionManager livySessionManager;

    private CustomerSpace customerSpace;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    protected String exportData() {
        ExportConfiguration exportConfig = setupExportConfig();
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfig);
        putStringValueInContext(EXPORT_DATA_APPLICATION_ID, submission.getApplicationIds().get(0));
        waitForAppId(submission.getApplicationIds().get(0));
        return exportConfig.getExportTargetPath();
    }

    private ExportConfiguration setupExportConfig() {
        ExportConfiguration exportConfig = new ExportConfiguration();
        exportConfig.setExportFormat(configuration.getExportFormat());
        exportConfig.setExportDestination(configuration.getExportDestination());
        exportConfig.setCustomerSpace(configuration.getCustomerSpace());
        exportConfig.setUsingDisplayName(configuration.getUsingDisplayName());
        exportConfig.setExclusionColumns(getExclusionColumns());
        exportConfig.setInclusionColumns(getInclusionColumns());
        exportConfig.setTable(retrieveTable());
        exportConfig.setExportInputPath(getExportInputPath());
        Map<String, String> properties = configuration.getProperties();
        if (StringUtils.isNotBlank(getExportOutputPath())) {
            exportConfig.setExportTargetPath(getExportOutputPath());
        } else if (properties.containsKey(ExportProperty.TARGET_FILE_NAME)) {
            String targetPath = PathBuilder
                    .buildDataFileExportPath(CamilleEnvironment.getPodId(), configuration.getCustomerSpace())
                    .append(properties.get(ExportProperty.TARGET_FILE_NAME)).toString();
            exportConfig.setExportTargetPath(targetPath);
            saveOutputValue(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH, targetPath);
        }
        for (String propertyName : configuration.getProperties().keySet()) {
            exportConfig.setProperty(propertyName, configuration.getProperties().get(propertyName));
        }
        return exportConfig;
    }

    private Table retrieveTable() {
        String tableName = getTableName();
        return metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
    }

    protected String getExclusionColumns() {
        return null;
    }

    protected String getInclusionColumns() {
        return null;
    }

    protected abstract String getTableName();

    protected abstract String getExportInputPath();

    protected abstract String getExportOutputPath();

    protected void mergeCSVFiles() {
        mergeCSVFiles(false);
    }

    /**
     * merged filename in hdfs:
     * keepMergedFileName=true -> will use configuration.getMergedFileName()
     * keepMergedFileName=false -> will use mergeToPath if it's a file path
     */
    protected void mergeCSVFiles(boolean keepMergedFileName) {
        customerSpace = configuration.getCustomerSpace();
        String tenant = customerSpace.toString();
        log.info("Tenant={}", tenant);
        String mergeToPath = configuration.getExportTargetPath();
        if (StringUtils.isNotBlank(getStringValueFromContext(EXPORT_MERGE_FILE_PATH))) {
            mergeToPath = getStringValueFromContext(EXPORT_MERGE_FILE_PATH);
        }
        log.info("MergeTo={}", mergeToPath);
        String mergedFileName = configuration.getMergedFileName();
        if (StringUtils.isNotBlank(getStringValueFromContext(EXPORT_MERGE_FILE_NAME))) {
            mergedFileName = getStringValueFromContext(EXPORT_MERGE_FILE_NAME);
        }
        putStringValueInContext(MERGED_FILE_NAME, mergedFileName);
        log.info("MergedFileName={}", mergedFileName);
        try {
            int lastSlashPos = mergeToPath.lastIndexOf('/');
            String pathToLookFor = mergeToPath.substring(0, lastSlashPos);
            String filePrefix = mergeToPath.substring(lastSlashPos + 1);
            String dstPath = keepMergedFileName ? pathToLookFor : mergeToPath;
            log.info("PathToLookFor={}", pathToLookFor);
            log.info("FilePrefix={}", filePrefix);
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration, pathToLookFor, filePrefix + "_.*.csv$");
            log.info("CSV files={}", JsonUtils.serialize(csvFiles));
            for (String file : csvFiles) {
                if (!keepMergedFileName) {
                    HdfsUtils.moveFile(yarnConfiguration, file, mergeToPath);
                }
            }
            MergeCSVConfig mergeCSVConfig = getMergeCSVConfig(dstPath);
            SparkJobResult result = SparkUtils.runJob(customerSpace, yarnConfiguration, sparkJobService,
                    livySessionManager, MergeCSVJob.class, mergeCSVConfig);
            HdfsDataUnit hdfsDataUnit = result.getTargets().get(0);
            String outputDir = hdfsDataUnit.getPath();
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv"));
            String csvPath = files.get(0);
            String renamePath = outputDir + "/" + mergedFileName;
            HdfsUtils.rename(yarnConfiguration, csvPath, renamePath);
            // mv generated csv file to dest path
            HdfsUtils.moveFile(yarnConfiguration, renamePath, dstPath);
            HdfsUtils.rmdir(yarnConfiguration, mergeCSVConfig.getWorkspace());
            if (keepMergedFileName) {
                // remove original files
                csvFiles.forEach(path -> {
                    try {
                        HdfsUtils.rmdir(yarnConfiguration, path);
                    } catch (IOException e) {
                        log.error("Failed to delete path {}, error = {}", path, e);
                    }
                });
                log.info("Removing original files {} after merged", csvFiles);
            }
            log.info("Done merging CSV files.");
        } catch (Exception e) {
            log.warn("Failed to merge csv files", e);
        }
    }

    private MergeCSVConfig getMergeCSVConfig(String path) {
        MergeCSVConfig mergeCSVConfig = new MergeCSVConfig();
        HdfsDataUnit hdfsDataUnit = HdfsDataUnit.fromPath(path);
        hdfsDataUnit.setDataFormat(DataUnit.DataFormat.CSV);
        mergeCSVConfig.setInput(Collections.singletonList(hdfsDataUnit));
        mergeCSVConfig.setWorkspace(PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString());
        return mergeCSVConfig;
    }

}
