package com.latticeengines.cdl.workflow.steps.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ExportEntity;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.EntityExportStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.common.ConvertToCSVConfig;
import com.latticeengines.serviceflows.workflow.dataflow.RunSparkJob;
import com.latticeengines.spark.exposed.job.common.ConvertToCSVJob;


@Component("saveAtlasExportCSV")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaveAtlasExportCSV extends RunSparkJob<EntityExportStepConfiguration, ConvertToCSVConfig, ConvertToCSVJob> {

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
            log.info("Submit spark job to convert " + exportEntity + " csv.");
            SparkJobResult result = sparkJobService.runJob(session, getJobClz(), config);
            outputUnits.put(exportEntity, result.getTargets().get(0));
        });
        return null;
    }

    private Map<String, String> getDisplayNameMap(ExportEntity exportEntity) {
        // use serving store proxy
        return null;
    }

    private Map<String, String> getDateAttrFmtMap(ExportEntity exportEntity) {
        // use serving store proxy, find all date attrs, set a format
        return null;
    }

    @Override
    protected void postJobExecution(SparkJobResult result) {
        outputUnits.forEach(((exportEntity, hdfsDataUnit) -> {
            String outputDir = hdfsDataUnit.getPath();
            String csvGzPath;
            try {
                List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, //
                        (HdfsUtils.HdfsFilenameFilter) filename -> filename.endsWith(".csv.gz"));
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
        saveToDataFiles(exportEntity, csvGzFilePath);
        if (configuration.isSaveToDropfolder()) {
            saveToDropfolder(exportEntity, csvGzFilePath);
        }
    }

    private void saveToDataFiles(ExportEntity exportEntity, String csvGzFilePath) {

    }

    private void saveToDropfolder(ExportEntity exportEntity, String csvGzFilePath) {

    }

    private void saveSupportingFilesToDropfolder() {

    }

}
