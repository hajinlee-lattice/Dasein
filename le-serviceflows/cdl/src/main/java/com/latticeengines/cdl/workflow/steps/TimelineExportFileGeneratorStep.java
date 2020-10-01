package com.latticeengines.cdl.workflow.steps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import com.latticeengines.common.exposed.transformer.TimelineExportAvroToCsvTransformer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.TimelineExportFileGeneratorConfiguration;
import com.latticeengines.domain.exposed.util.TimeLineStoreUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("timelineExportFileGeneratorStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class TimelineExportFileGeneratorStep extends BaseWorkflowStep<TimelineExportFileGeneratorConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(TimelineExportFileGeneratorStep.class);

    @Override
    public void execute() {
        TimelineExportFileGeneratorConfiguration config = getConfiguration();
        List<String> avroHdfsFilePaths = getListObjectFromContext(TIMELINE_EXPORT_TABLES, String.class);
        log.info("customerSpace is {}.", config.getCustomerSpace());
        try (PerformanceTimer timer = new PerformanceTimer(
                String.format("Generating time Export Files for:%s", config.getCustomerSpace()))) {

            config.setFieldNames(TimeLineStoreUtils.TimelineExportColumn.getColumnNames());
            log.info("timeline fieldNames: {}", config.getFieldNames());
            Date fileExportTime;
            List<Callable<String>> fileExporters = new ArrayList<>();

            for (String avroFilePath : avroHdfsFilePaths) {
                fileExportTime = new Date();
                String tableName = String.format("tl_export_%s", fileExportTime.getTime());
                fileExporters.add(new TimelineExportFileGeneratorStep.CsvFileExporter(yarnConfiguration, config,
                        avroFilePath, tableName));
            }

            ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("timeline-export", 2);
            List<String> exportFiles = ThreadPoolUtils.callInParallel(executorService, fileExporters,
                    (int) TimeUnit.DAYS.toMinutes(1), 30);
            if (exportFiles.size() != fileExporters.size()) {
                throw new RuntimeException("Failed to generate some of the export files");
            }
            log.info("exportFiles is {}.", exportFiles);
            putObjectInContext(TIMELINE_EXPORT_FILES, exportFiles);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_70001, e, new String[] { e.getMessage() });
        }

    }

    public String buildNamespace(TimelineExportFileGeneratorConfiguration config) {
        return config.getNameSpace();
    }

    private class CsvFileExporter extends ExportFileCallable {

        CsvFileExporter(Configuration yarnConfiguration, TimelineExportFileGeneratorConfiguration config,
                        String avroHdfsFilePath, String tablename) {
            super(yarnConfiguration, config, avroHdfsFilePath, tablename);
        }

        @Override
        public void generateFileFromAvro(String avroHdfsFilePath, File localFile) throws IOException {
            AvroUtils.convertAvroToCSV(yarnConfiguration, avroHdfsFilePath, localFile,
                    new TimelineExportAvroToCsvTransformer(config.getFieldNames()));
        }

        @Override
        public String getFileFormat() {
            return "csv";
        }

    }

    private abstract class ExportFileCallable implements Callable<String> {

        Configuration yarnConfiguration;
        TimelineExportFileGeneratorConfiguration config;
        String avroHdfsFilePath;
        String tableName;

        ExportFileCallable(Configuration yarnConfiguration, TimelineExportFileGeneratorConfiguration config,
                           String avroHdfsFilePath, String tableName) {
            this.yarnConfiguration = yarnConfiguration;
            this.config = config;
            this.avroHdfsFilePath = avroHdfsFilePath;
            this.tableName = tableName;
        }

        public abstract void generateFileFromAvro(String avroHdfsFilePath, File localFile) throws IOException;

        public abstract String getFileFormat();

        @Override
        public String call() {
            try {
                File localFile = new File(String.format("%s.%s", tableName, getFileFormat()));

                generateFileFromAvro(avroHdfsFilePath, localFile);

                String namespace = buildNamespace(config);
                String path = PathBuilder
                        .buildDataFileExportPath(CamilleEnvironment.getPodId(), config.getCustomerSpace(), namespace)
                        .toString();
                path = path.endsWith("/") ? path : path + "/";

                String filePathForDestination = (path + String.format("%s.%s",
                        tableName, getFileFormat()));

                try {
                    HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(),
                            filePathForDestination);
                } finally {
                    FileUtils.deleteQuietly(localFile);
                }
                log.debug("Uploaded timelineExport File to HDFS : {}", filePathForDestination);
                return filePathForDestination;
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_18213, e, new String[] { getFileFormat() });
            }
        }

    }
}
