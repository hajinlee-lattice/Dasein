package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.service.DataCloudNotificationService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.dataflow.runtime.cascading.propdata.SimpleCascadingExecutor;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.IngestedFileToSourceParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.IngestedFileToSourceTransformerConfig.CompressType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.SlackSettings;

@Component("ingestedFileToSourceDataFlowService")
public class IngestedFileToSourceDataFlowService extends AbstractTransformationDataFlowService {
    private static final Logger log = LoggerFactory.getLogger(IngestedFileToSourceDataFlowService.class);

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SimpleCascadingExecutor simpleCascadingExecutor;

    @Autowired
    private DataCloudNotificationService notificationService;

    public void executeDataFlow(Source source, String workflowDir, String baseVersion,
            IngestedFileToSourceParameters parameters) {
        CsvToAvroFieldMappingImpl fieldTypeMapping = new CsvToAvroFieldMappingImpl(parameters.getColumns());
        String ingestionDir = hdfsPathBuilder.constructIngestionDir(parameters.getIngestionName(), baseVersion)
                .toString();
        log.info("Ingestion Directory: " + ingestionDir);

        try {
            String searchDir = ingestionDir;
            List<String> files = null;
            Path uncompressedDir = new Path(ingestionDir, EngineConstants.UNCOMPRESSED);
            // If files are compressed, need to uncompress first
            if (StringUtils.isNotEmpty(parameters.getCompressedFileNameOrExtension())) {
                if (HdfsUtils.isDirectory(yarnConfiguration, uncompressedDir.toString())) {
                    HdfsUtils.rmdir(yarnConfiguration, uncompressedDir.toString());
                    log.info("Uncompressed dir " + uncompressedDir.toString() + " already exists. Delete first.");
                }
                HdfsUtils.mkdir(yarnConfiguration, uncompressedDir.toString());
                log.info("Uncompressed dir " + uncompressedDir.toString() + " created");
                searchDir = uncompressedDir.toString();
                log.info("Searching compressed files with name/extension as "
                        + parameters.getCompressedFileNameOrExtension() + " in directory " + ingestionDir);
                List<String> compressedFiles = scanDir(ingestionDir, parameters.getCompressedFileNameOrExtension(),
                        true, false);
                log.info("Found " + compressedFiles.size() + " compressed files.");
                uncompress(compressedFiles, uncompressedDir, parameters.getCompressType(),
                        parameters.getApplicationId());
            }
            log.info("Searching if target files with name/extension " + parameters.getFileNameOrExtension()
                    + " exist in dir " + searchDir);
            files = scanDir(searchDir, parameters.getFileNameOrExtension(), true, false);
            if (CollectionUtils.isEmpty(files)) {
                throw new RuntimeException("Fail to find target files with name/extension "
                        + parameters.getFileNameOrExtension() + " exist in dir " + searchDir);
            }
            log.info("Found " + files.size() + " target files.");
            Path path = new Path(files.get(0));
            String searchWildCard = new Path(path.getParent(), "*" + parameters.getFileNameOrExtension()).toString();
            log.info("SearchWildCard: " + searchWildCard);
            convertCsvToAvro(fieldTypeMapping, searchWildCard, workflowDir, parameters);
            if (HdfsUtils.isDirectory(yarnConfiguration, uncompressedDir.toString())) {
                HdfsUtils.rmdir(yarnConfiguration, uncompressedDir.toString());
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
        }
    }

    private void uncompress(List<String> compressedFiles, Path uncompressedDir, CompressType type, String applicationId)
            throws IOException {
        BlockingQueue<Runnable> runnableQueue = new LinkedBlockingQueue<Runnable>();
        ExecutorService uncompressExecutor = new ThreadPoolExecutor(4, 8, 1, TimeUnit.MINUTES, runnableQueue);
        List<Future<Pair<String, Boolean>>> futures = new ArrayList<>();
        for (String compressedFile : compressedFiles) {
            Callable<Pair<String, Boolean>> task = createUncompressCallable(compressedFile, uncompressedDir, type);
            Future<Pair<String, Boolean>> future = uncompressExecutor.submit(task);
            futures.add(future);
        }
        for (Future<Pair<String, Boolean>> future : futures) {
            Pair<String, Boolean> res;
            try {
                res = future.get(2, TimeUnit.HOURS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            if (!Boolean.TRUE.equals(res.getRight())) {
                // Don't fail the entire job because some data provider (eg.
                // Bombora) often provides 1 or 2 corrupted compressed file out
                // of 50+ as total. Don't want manual operation every time. But
                // send both email and slack message as alerts to raise
                // attention.
                // If by chance all the files are failed to be uncompressed,
                // csv-to-avro job will fail.
                sendUncompressFailureNotification(res.getLeft(), applicationId);
            }
        }
        uncompressExecutor.shutdown();
    }

    private Callable<Pair<String, Boolean>> createUncompressCallable(String compressedFile, Path uncompressedDir,
            CompressType type) {
        Callable<Pair<String, Boolean>> task = new Callable<Pair<String, Boolean>>() {
            @Override
            public Pair<String, Boolean> call() {
                try {
                    uncompress(compressedFile, uncompressedDir, type);
                } catch (IOException e) {
                    log.error("Failed to uncompress file " + compressedFile, e);
                    return Pair.of(compressedFile, Boolean.FALSE);
                }
                return Pair.of(compressedFile, Boolean.TRUE);
            }
        };
        return task;
    }

    private void uncompress(String compressedFile, Path uncompressedDir, CompressType type) throws IOException {
        Path compressedPath = new Path(compressedFile);
        log.info("CompressedPath: " + compressedFile);
        switch (type) {
        case GZ:
            String compressedNameOnly = compressedPath.getName();
            String uncompressedNameOnly = StringUtils.endsWithIgnoreCase(compressedNameOnly, EngineConstants.GZ)
                    ? compressedNameOnly.substring(0, compressedNameOnly.length() - EngineConstants.GZ.length())
                    : compressedNameOnly;
            Path uncompressedPath = new Path(uncompressedDir, uncompressedNameOnly);
            log.info("UncompressedPath for gz file: " + uncompressedPath.toString());
            try {
                HdfsUtils.uncompressGZFileWithinHDFS(yarnConfiguration, compressedFile, uncompressedPath.toString());
            } catch (Exception e) {
                // Delete partial uncompressed file
                if (HdfsUtils.fileExists(yarnConfiguration, uncompressedPath.toString())) {
                    HdfsUtils.rmdir(yarnConfiguration, uncompressedPath.toString());
                }
                throw e;
            }
            break;
        case ZIP:
            log.info("UncompressedPath for zip file: " + uncompressedDir.toString());
            HdfsUtils.uncompressZipFileWithinHDFS(yarnConfiguration, compressedFile, uncompressedDir.toString());
            break;
        default:
            throw new RuntimeException("CompressType " + type.name() + "  is not supported");
        }
    }

    private List<String> scanDir(String inputDir, final String suffix, boolean recursive, boolean returnFirstMatch)
            throws IOException {
        HdfsFileFilter filter = new HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus fileStatus) {
                return fileStatus.getPath().getName().endsWith(suffix);
            }
        };

        List<String> files = null;
        if (recursive) {
            files = HdfsUtils.onlyGetFilesForDirRecursive(yarnConfiguration, inputDir, filter, returnFirstMatch);
        } else {
            files = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, inputDir, filter);
        }
        List<String> resultFiles = new ArrayList<String>();

        if (!CollectionUtils.isEmpty(files) && files.size() > 0) {
            for (String fullPath : files) {
                if (fullPath.startsWith(inputDir)) {
                    resultFiles.add(fullPath);
                } else {
                    if (fullPath.contains(inputDir)) {
                        resultFiles.add(fullPath.substring(fullPath.indexOf(inputDir)));
                    }
                }
            }
        }
        return resultFiles;
    }

    private void convertCsvToAvro(CsvToAvroFieldMapping fieldTypeMapping, String inputPath, String outputPath,
            IngestedFileToSourceParameters parameters) throws IOException {
        simpleCascadingExecutor.transformCsvToAvro(fieldTypeMapping, inputPath, outputPath, parameters.getDelimiter(),
                parameters.getQualifier(), parameters.getCharset(), false);
    }

    private void sendUncompressFailureNotification(String file, String applicationId) {
        String subject = "File process failure notification [" + applicationId + "]";
        String content = "Fail to uncompress file " + file + ". Current job has skipped the file to proceed. "
                + "No need manual operation now, but please pay attention or contact data provider.";
        notificationService.sendSlack(subject, content, "IngestedFileToSourceTransformer", SlackSettings.Color.DANGER);
        notificationService.sendEmail(subject, content, null);
    }
}
