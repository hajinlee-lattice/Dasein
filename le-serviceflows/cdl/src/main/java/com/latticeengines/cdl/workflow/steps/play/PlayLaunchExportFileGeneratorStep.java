package com.latticeengines.cdl.workflow.steps.play;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchExportFilesGeneratorConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("playLaunchExportFileGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PlayLaunchExportFileGeneratorStep extends BaseWorkflowStep<PlayLaunchExportFilesGeneratorConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchExportFileGeneratorStep.class);

    @Override
    public void execute() {
        PlayLaunchExportFilesGeneratorConfiguration config = getConfiguration();
        String recAvroHdfsFilePath = getStringValueFromContext(
                PlayLaunchWorkflowConfiguration.RECOMMENDATION_CSV_EXPORT_AVRO_HDFS_FILEPATH);

        try (PerformanceTimer timer = new PerformanceTimer(
                String.format("Generating PlayLaunch Export Files for:%s", config.getPlayName()))) {

            // Replace account and contact display names with values in context.
            // For old PlayLaunchWorkflow, these two maps should be null.
            // When we migrate to use CampaignLaunchWorkflow entirely, we can
            // either remove the if check or set those two maps in submitter.
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

            List<Callable<String>> fileExporters = new ArrayList<>();
            Date fileExportTime = new Date();
            fileExporters.add(new CsvFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));
            fileExporters.add(new JsonFileExporter(yarnConfiguration, config, recAvroHdfsFilePath, fileExportTime));

            ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("playlaunch-export", 2);
            List<String> exportFiles = ThreadPoolUtils.callInParallel(executorService, fileExporters,
                    (int) TimeUnit.DAYS.toMinutes(1), 30);
            if (exportFiles.size() != fileExporters.size()) {
                throw new RuntimeException("Failed to generate some of the export files");
            }
            putObjectInContext(PlayLaunchWorkflowConfiguration.RECOMMENDATION_EXPORT_FILES, exportFiles);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18213, e, new String[] { e.getMessage() });
        }
    }

    public String buildNamespace(PlayLaunchExportFilesGeneratorConfiguration config) {
        return String.format("%s.%s.%s.%s.%s", config.getDestinationSysType(), config.getDestinationSysName(),
                config.getDestinationOrgId(), config.getPlayName(), config.getPlayLaunchId());
    }

    private class CsvFileExporter extends ExportFileCallable {

        CsvFileExporter(Configuration yarnConfiguration, PlayLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date) {
            super(yarnConfiguration, config, recAvroHdfsFilePath, date);
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

    private boolean shouldIgnoreAccountsWithoutContacts(PlayLaunchExportFilesGeneratorConfiguration config) {
        return config.getDestinationSysType() == CDLExternalSystemType.MAP
                || ChannelConfigUtil.isContactAudienceType(config.getDestinationSysName(), config.getChannelConfig());
    }

    private class JsonFileExporter extends ExportFileCallable {

        JsonFileExporter(Configuration yarnConfiguration, PlayLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date) {
            super(yarnConfiguration, config, recAvroHdfsFilePath, date);
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
        PlayLaunchExportFilesGeneratorConfiguration config;
        Date fileGeneratedTime;
        String recAvroHdfsFilePath;

        ExportFileCallable(Configuration yarnConfiguration, PlayLaunchExportFilesGeneratorConfiguration config,
                String recAvroHdfsFilePath, Date date) {
            this.yarnConfiguration = yarnConfiguration;
            this.config = config;
            this.fileGeneratedTime = date;
            this.recAvroHdfsFilePath = recAvroHdfsFilePath;
        }

        public abstract void generateFileFromAvro(String recAvroHdfsFilePath, File localFile) throws IOException;

        public abstract String getFileFormat();

        @Override
        public String call() {
            try {
                File localFile = new File(String.format("pl_rec_%s_%s_%s_%s.%s",
                        config.getCustomerSpace().getTenantId(), config.getPlayLaunchId(),
                        config.getDestinationSysType(), fileGeneratedTime.getTime(), getFileFormat()));

                generateFileFromAvro(recAvroHdfsFilePath, localFile);

                String namespace = buildNamespace(config);
                String path = PathBuilder
                        .buildDataFileExportPath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), namespace)
                        .toString();
                path = path.endsWith("/") ? path : path + "/";

                String recFilePathForDestination = (path + String.format("Recommendations_%s.%s",
                        DateTimeUtils.currentTimeAsString(fileGeneratedTime), getFileFormat()));

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
