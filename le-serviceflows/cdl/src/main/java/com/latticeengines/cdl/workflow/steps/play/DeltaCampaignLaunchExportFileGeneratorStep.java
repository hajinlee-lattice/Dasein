package com.latticeengines.cdl.workflow.steps.play;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToCsvTransformer;
import com.latticeengines.common.exposed.transformer.RecommendationAvroToJsonFunction;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.DeltaCampaignLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("deltaCampaignLaunchExportFileGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeltaCampaignLaunchExportFileGeneratorStep
        extends BaseWorkflowStep<DeltaCampaignLaunchExportFilesGeneratorConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchExportFileGeneratorStep.class);

    private boolean createAddCsvDataFrame;

    private boolean createDeleteCsvDataFrame;

    private static final String ADD_FILE_PREFIX = "add";

    private static final String DELETE_FILE_PREFIX = "delete";

    @Override
    public void execute() {
        DeltaCampaignLaunchExportFilesGeneratorConfiguration config = getConfiguration();
        createAddCsvDataFrame = Boolean.toString(true)
                .equals(getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        createDeleteCsvDataFrame = Boolean.toString(true).equals(
                getStringValueFromContext(DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));
        log.info("createAddCsvDataFrame=" + createAddCsvDataFrame + ", createDeleteCsvDataFrame="
                + createDeleteCsvDataFrame);
        Map<String, String> accountDisplayNames = getMapObjectFromContext(RECOMMENDATION_ACCOUNT_DISPLAY_NAMES,
                String.class, String.class);
        Map<String, String> contactDisplayNames = getMapObjectFromContext(RECOMMENDATION_CONTACT_DISPLAY_NAMES,
                String.class, String.class);
        log.info("accountDisplayNames map: " + accountDisplayNames);
        log.info("contactDisplayNames map: " + contactDisplayNames);
        if (accountDisplayNames != null) {
            config.setAccountDisplayNames(accountDisplayNames);
        }
        if (contactDisplayNames != null) {
            config.setContactDisplayNames(contactDisplayNames);
        }

        try (PerformanceTimer timer = new PerformanceTimer(
                String.format("Generating Delta Campaign Launch Export Files for:%s", config.getPlayName()))) {

            if (createAddCsvDataFrame) {
                String addCsvDataFraneHdfsFilePath = getStringValueFromContext(
                        DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH);
                List<Callable<String>> fileExporters = new ArrayList<>();
                Date fileExportTime = new Date();
                fileExporters.add(new CsvFileExporter(yarnConfiguration, config, addCsvDataFraneHdfsFilePath,
                        fileExportTime, ADD_FILE_PREFIX));
                fileExporters.add(new JsonFileExporter(yarnConfiguration, config, addCsvDataFraneHdfsFilePath,
                        fileExportTime, ADD_FILE_PREFIX));

                ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("deltaCampaignlaunch-export",
                        2);
                List<String> exportFiles = ThreadPoolUtils.callInParallel(executorService, fileExporters,
                        (int) TimeUnit.DAYS.toMinutes(1), 30);
                if (exportFiles.size() != fileExporters.size()) {
                    throw new RuntimeException("Failed to generate some of the export files");
                }
                log.info("Export files for add csv " + Arrays.toString(exportFiles.toArray()));
                putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_FILES, exportFiles);
            }

            if (createDeleteCsvDataFrame) {
                String deleteCsvDataFraneHdfsFilePath = getStringValueFromContext(
                        DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH);
                List<Callable<String>> fileExporters = new ArrayList<>();
                Date fileExportTime = new Date();
                fileExporters.add(new CsvFileExporter(yarnConfiguration, config, deleteCsvDataFraneHdfsFilePath,
                        fileExportTime, DELETE_FILE_PREFIX));

                ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("deltaCampaignlaunch-export",
                        2);
                List<String> exportFiles = ThreadPoolUtils.callInParallel(executorService, fileExporters,
                        (int) TimeUnit.DAYS.toMinutes(1), 30);
                if (exportFiles.size() != fileExporters.size()) {
                    throw new RuntimeException("Failed to generate some of the export files");
                }
                log.info("Export files for delete csv " + Arrays.toString(exportFiles.toArray()));
                putObjectInContext(DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_FILES, exportFiles);
            }

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18213, e, new String[] { e.getMessage() });
        }
    }

    public String buildNamespace(DeltaCampaignLaunchExportFilesGeneratorConfiguration config) {
        return String.format("%s.%s.%s.%s.%s", config.getDestinationSysType(), config.getDestinationSysName(),
                config.getDestinationOrgId(), config.getPlayName(), config.getPlayLaunchId());
    }

    private class CsvFileExporter extends ExportFileCallable {

        CsvFileExporter(Configuration yarnConfiguration, DeltaCampaignLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date, String filePrefix) {
            super(yarnConfiguration, config, recAvroHdfsFilePath, date, filePrefix);
        }

        @Override
        public void generateFileFromAvro(String recAvroHdfsFilePath, File localFile) throws IOException {
            AvroUtils.convertAvroToCSV(yarnConfiguration, recAvroHdfsFilePath, localFile,
                    new RecommendationAvroToCsvTransformer(config.getAccountDisplayNames(),
                            config.getContactDisplayNames(), shouldIgnoreAccountsWithoutContacts(config)));
        }

        @Override
        public String getFileFormat() {
            return "csv";
        }

    }

    private boolean shouldIgnoreAccountsWithoutContacts(DeltaCampaignLaunchExportFilesGeneratorConfiguration config) {
        return config.getDestinationSysType() == CDLExternalSystemType.MAP
                || ChannelConfigUtil.isContactAudienceType(config.getDestinationSysName(), config.getChannelConfig());
    }

    private class JsonFileExporter extends ExportFileCallable {

        JsonFileExporter(Configuration yarnConfiguration, DeltaCampaignLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date, String filePrefix) {
            super(yarnConfiguration, config, recAvroHdfsFilePath, date, filePrefix);
        }

        @Override
        public void generateFileFromAvro(String recAvroHdfsFilePath, File localFile) throws IOException {
            AvroUtils.convertAvroToJSON(yarnConfiguration, recAvroHdfsFilePath, localFile,
                    new RecommendationAvroToJsonFunction(config.getAccountDisplayNames(),
                            config.getContactDisplayNames()));
        }

        @Override
        public String getFileFormat() {
            return "json";
        }
    }

    private abstract class ExportFileCallable implements Callable<String> {

        Configuration yarnConfiguration;
        DeltaCampaignLaunchExportFilesGeneratorConfiguration config;
        Date fileGeneratedTime;
        String recAvroHdfsFilePath;
        String filePrefix;

        ExportFileCallable(Configuration yarnConfiguration, DeltaCampaignLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date, String filePrefix) {
            this.yarnConfiguration = yarnConfiguration;
            this.config = config;
            this.fileGeneratedTime = date;
            this.recAvroHdfsFilePath = recAvroHdfsFilePath;
            this.filePrefix = filePrefix;
        }

        public abstract void generateFileFromAvro(String recAvroHdfsFilePath, File localFile) throws IOException;

        public abstract String getFileFormat();

        @Override
        public String call() {
            try {
                File localFile = new File(String.format("pl_rec_%s_%s_%s_%s_%s.%s", filePrefix,
                        config.getCustomerSpace().getTenantId(), config.getPlayLaunchId(),
                        config.getDestinationSysType(), fileGeneratedTime.getTime(), getFileFormat()));

                generateFileFromAvro(recAvroHdfsFilePath, localFile);

                String namespace = buildNamespace(config);
                String path = PathBuilder
                        .buildDataFileExportPath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), namespace)
                        .toString();
                path = path.endsWith("/") ? path : path + "/";

                String recFilePathForDestination = (path += String.format("Recommendations_%s_%s.%s", filePrefix,
                        DateTimeUtils.currentTimeAsString(fileGeneratedTime), getFileFormat()));
                log.info("recFilePathForDestination : {}", recFilePathForDestination);
                try {
                    HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(),
                            recFilePathForDestination);
                } finally {
                    FileUtils.deleteQuietly(localFile);
                }
                log.debug("Uploaded PlayLaunch File to HDFS : {}", recFilePathForDestination);
                return recFilePathForDestination;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18213, e, new String[] { getFileFormat() });
            }
        }

    }
}
