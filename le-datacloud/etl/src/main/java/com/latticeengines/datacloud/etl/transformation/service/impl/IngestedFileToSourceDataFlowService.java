package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
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


@Component("ingestedFileToSourceDataFlowService")
public class IngestedFileToSourceDataFlowService extends AbstractTransformationDataFlowService {
    private static final Logger log = LoggerFactory.getLogger(IngestedFileToSourceDataFlowService.class);

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SimpleCascadingExecutor simpleCascadingExecutor;

    public void executeDataFlow(Source source, String workflowDir, String baseVersion,
            IngestedFileToSourceParameters parameters) {
        CsvToAvroFieldMappingImpl fieldTypeMapping = new CsvToAvroFieldMappingImpl(parameters.getColumns());
        String ingestionDir = hdfsPathBuilder.constructIngestionDir(parameters.getIngestionName(), baseVersion)
                .toString();
        log.info("Ingestion Directory: " + ingestionDir);
        
        try {
            boolean foundFiles = false;
            String searchDir = ingestionDir;
            List<String> files = null;
            Path uncompressedDir = new Path(ingestionDir, EngineConstants.UNCOMPRESSED);
            if (StringUtils.isNotEmpty(parameters.getCompressedFileNameOrExtension())
                    && !HdfsUtils.isDirectory(yarnConfiguration, uncompressedDir.toString())) {
                log.info("Uncompressed dir " + uncompressedDir.toString() + " does not exist. Create one.");
                HdfsUtils.mkdir(yarnConfiguration, uncompressedDir.toString());
                searchDir = uncompressedDir.toString();
                log.info("Searching compressed files with name/extension as "
                        + parameters.getCompressedFileNameOrExtension() + " in directory " + ingestionDir);
                List<String> compressedFiles = scanDir(ingestionDir, parameters.getCompressedFileNameOrExtension(),
                        true, false);
                log.info("Found " + compressedFiles.size() + " compressed files.");
                uncompress(compressedFiles, uncompressedDir, parameters.getCompressType());
            } else if (StringUtils.isNotEmpty(parameters.getCompressedFileNameOrExtension())
                    && HdfsUtils.isDirectory(yarnConfiguration, uncompressedDir.toString())) {
                log.info("Uncompressed dir " + uncompressedDir.toString() + " already exists.");
                searchDir = uncompressedDir.toString();
                log.info("Searching if target files with name/extension " + parameters.getFileNameOrExtension()
                        + " exist in dir " + searchDir);
                files = scanDir(searchDir, parameters.getFileNameOrExtension(), true, true);
                if (!CollectionUtils.isEmpty(files)) {
                    log.info("Target files exist already.");
                    foundFiles = true;
                }
                if (!foundFiles) {
                    log.info("Target files do not exist. Remove " + uncompressedDir.toString()
                            + " and create a new one.");
                    HdfsUtils.rmdir(yarnConfiguration, uncompressedDir.toString());
                    HdfsUtils.mkdir(yarnConfiguration, uncompressedDir.toString());
                    log.info("Searching compressed files with name/extension as "
                            + parameters.getCompressedFileNameOrExtension() + " in directory " + ingestionDir);
                    List<String> compressedFiles = scanDir(ingestionDir, parameters.getCompressedFileNameOrExtension(),
                            true, false);
                    log.info("Found " + compressedFiles.size() + " compressed files.");
                    uncompress(compressedFiles, uncompressedDir, parameters.getCompressType());
                }
            }
            if (!foundFiles) {
                log.info("Searching if target files with name/extension " + parameters.getFileNameOrExtension()
                        + " exist in dir " + searchDir);
                files = scanDir(searchDir, parameters.getFileNameOrExtension(), true, true);
                if (!CollectionUtils.isEmpty(files)) {
                    foundFiles = true;
                }
            }
            if (foundFiles) {
                log.info("Found target files.");
                Path path = new Path(files.get(0));
                String searchWildCard = new Path(path.getParent(), "*" + parameters.getFileNameOrExtension())
                        .toString();
                log.info("SearchWildCard: " + searchWildCard);
                convertCsvToAvro(fieldTypeMapping, searchWildCard, workflowDir, parameters);
            } else {
                log.error("Fail to find any target files!");
            }

        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
        }
    }

    private void uncompress(List<String> compressedFiles, Path uncompressedDir, CompressType type)
            throws IOException {
        for (String compressedFile : compressedFiles) {
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
                HdfsUtils.uncompressGZFileWithinHDFS(yarnConfiguration, compressedFile, uncompressedPath.toString());
                break;
            case ZIP:
                log.info("UncompressedPath for zip file: " + uncompressedDir.toString());
                HdfsUtils.uncompressZipFileWithinHDFS(yarnConfiguration, compressedFile, uncompressedDir.toString());
                break;
            default:
                throw new RuntimeException("CompressType " + type.name() + "  is not supported");
            }
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
}
