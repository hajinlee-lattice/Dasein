package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.datacloud.core.source.DataImportedFromHDFS;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.SimpleCascadingExecutor;
import com.latticeengines.domain.exposed.datacloud.EngineConstants;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.FileInputSourceConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("firehoseTransformationDataFlowService")
public class FirehoseTransformationDataFlowService extends AbstractTransformationDataFlowService {

    private static final Logger log = LoggerFactory.getLogger(FirehoseTransformationDataFlowService.class);

    private static final String AVRO_DIR_FOR_CONVERSION = "AVRO_DIR_FOR_CONVERSION";
    private static final String UNCOMPRESSED_FILE = "UNCOMPRESSED-";

    @Autowired
    private SimpleCascadingExecutor simpleCascadingExecutor;

    private CsvToAvroFieldMapping fieldTypeMapping;

    public void executeDataProcessing(Source source, String workflowDir, String baseVersion,
                                      String uid, String dataFlowBean,
                                      TransformationConfiguration transformationConfiguration) {
        if (StringUtils.isEmpty(dataFlowBean) || fieldTypeMapping == null) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(),
                            (fieldTypeMapping == null ? "CsvToAvroFieldMapping cannot be null"
                                    : "Name of FlowBean cannot be null") });
        }

        FileInputSourceConfig inputConfig = null;
        try {
            inputConfig = (FileInputSourceConfig) transformationConfiguration
                    .getInputSourceConfig();
        } catch (UnsupportedOperationException e1) {
            throw new LedpException(LedpCode.LEDP_25022, e1);
        }

        if (source instanceof DataImportedFromHDFS) {
            String inputDir = ((DataImportedFromHDFS) source).getHDFSPathToImportFrom()
                    .append(baseVersion).toString();
            log.info("Ingestion DIR: " + inputDir);
            List<String> gzHdfsPaths = null;

            try {
                gzHdfsPaths = scanDir(inputDir, inputConfig.getExtension());
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
            }

            try {
                for (int i = 0; i < gzHdfsPaths.size(); i++) {
                    String gzHdfsPath = gzHdfsPaths.get(i);
                    String uncompressedFilePath = new Path(workflowDir,
                            UNCOMPRESSED_FILE + String.format("%04d", i) + EngineConstants.CSV)
                                    .toString();
                    log.info("UncompressedFilePath: " + uncompressedFilePath);
                    untarGZFile(gzHdfsPath, uncompressedFilePath);
                }

                String avroDirPath = new Path(workflowDir, AVRO_DIR_FOR_CONVERSION).toString();
                log.info("AvroDirPath: " + avroDirPath);
                String uncompressedFilePathWildcard = new Path(workflowDir,
                        UNCOMPRESSED_FILE + "*" + EngineConstants.CSV).toString();
                log.info("uncompressedFilePathWildcard: " + uncompressedFilePathWildcard);
                convertCsvToAvro(fieldTypeMapping, uncompressedFilePathWildcard, avroDirPath,
                        inputConfig);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
            }

            /*
            for (int i = 0; i < gzHdfsPaths.size(); i++) {
                String gzHdfsPath = gzHdfsPaths.get(i);
                String uncompressedFilePath = new Path(workflowDir,
                        UNCOMPRESSED_FILE + String.format("%04d", i) + EngineConstants.CSV)
                                .toString();
                log.info("UncompressedFilePath: " + uncompressedFilePath);
                String avroDirPath = new Path(workflowDir, new Path(AVRO_DIR_FOR_CONVERSION,
                        UNCOMPRESSED_FILE + String.format("%04d", i))).toString();
                log.info("AvroDirPath: " + avroDirPath);

                try {
                    untarGZFile(gzHdfsPath, uncompressedFilePath);
                    convertCsvToAvro(fieldTypeMapping, uncompressedFilePath, avroDirPath,
                            inputConfig);
                    List<String> avroFilePaths = scanDir(avroDirPath, EngineConstants.AVRO);
                    for (String avroFilePath : avroFilePaths) {
                        Path srcAvroFilePath = new Path(avroFilePath);
                        Path dstAvroFilePath = new Path(
                                new Path(workflowDir, AVRO_DIR_FOR_CONVERSION),
                                UNCOMPRESSED_FILE + String.format("%04d", i) + "-"
                                        + srcAvroFilePath.getName());
                        HdfsUtils.moveFile(yarnConfiguration, avroFilePath,
                                dstAvroFilePath.toString());
                        log.info("SrcAvroFilePath: " + srcAvroFilePath.toString());
                        log.info("DstAvroFilePath: " + dstAvroFilePath.toString());
                    }
                } catch (IOException e) {
                    throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
                }
            }
            */
        }
    }

    private List<String> scanDir(String inputDir, final String suffix) throws IOException {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                return filename.endsWith(suffix);
            }
        };

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, inputDir, filter);
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

    private void untarGZFile(String gzHdfsPath, String uncompressedFilePath) throws IOException {
        HdfsUtils.uncompressGZFileWithinHDFS(yarnConfiguration, gzHdfsPath, uncompressedFilePath);
    }

    private void convertCsvToAvro(CsvToAvroFieldMapping fieldTypeMapping,
            String uncompressedFilePath, String avroDirPath, FileInputSourceConfig inputConfig)
                    throws IOException {
        simpleCascadingExecutor.transformCsvToAvro(fieldTypeMapping, uncompressedFilePath, avroDirPath,
                inputConfig.getDelimiter(), inputConfig.getQualifier(), inputConfig.getCharset(), true);
    }

    public void setFieldTypeMapping(CsvToAvroFieldMapping fieldTypeMapping) {
        this.fieldTypeMapping = fieldTypeMapping;
    }

}
