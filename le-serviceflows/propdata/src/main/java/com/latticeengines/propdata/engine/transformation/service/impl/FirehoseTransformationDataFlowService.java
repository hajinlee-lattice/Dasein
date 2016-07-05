package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.SimpleCascadingExecutor;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

@Component("firehoseTransformationDataFlowService")
public class FirehoseTransformationDataFlowService extends AbstractTransformationDataFlowService {

    private static final String HDFS_PATH_SEPARATOR = "/";

    private static final String AVRO_DIR_FOR_CONVERSION = "AVRO_DIR_FOR_CONVERSION";

    private static final String CSV_EXTENSION = ".csv";

    private static final String PART_FILE = "PART-0001";

    private static final String CSV_GZ = ".csv.gz";

    @Autowired
    private SimpleCascadingExecutor simpleCascadingExecutor;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.collection.cascading.platform:tez}")
    private String cascadingPlatform;

    private CsvToAvroFieldMapping fieldTypeMapping;

    @Override
    public void executeDataProcessing(Source source, String workflowDir, String baseVersion, String uid,
            String dataFlowBean, TransformationConfiguration transformationConfiguration) {
        if (StringUtils.isEmpty(dataFlowBean) || fieldTypeMapping == null) {
            throw new LedpException(LedpCode.LEDP_25012,
                    new String[] { source.getSourceName(), (fieldTypeMapping == null
                            ? "CsvToAvroFieldMapping cannot be null" : "Name of FlowBean cannot be null") });
        }

        if (source instanceof DataImportedFromHDFS) {
            String inputDir = hdfsPathBuilder.constructIngestionDir(source.getSourceName(), baseVersion).toString();
            String gzHdfsPath = null;

            try {
                gzHdfsPath = scanDir(inputDir, CSV_GZ);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
            }

            String uncompressedFilePath = workflowDir + HDFS_PATH_SEPARATOR + PART_FILE + CSV_EXTENSION;
            String avroDirPath = workflowDir + HDFS_PATH_SEPARATOR + AVRO_DIR_FOR_CONVERSION;

            try {
                untarGZFile(gzHdfsPath, uncompressedFilePath);
                convertCsvToAvro(fieldTypeMapping, uncompressedFilePath, avroDirPath);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
            }
        }
    }

    private String scanDir(String inputDir, final String suffix) throws IOException {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                return filename.endsWith(suffix);
            }
        };

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, inputDir, filter);

        if (!CollectionUtils.isEmpty(files) && files.size() > 0) {
            String fullPath = files.get(0);
            if (fullPath.startsWith(inputDir)) {
                return fullPath;
            } else {
                if (fullPath.contains(fullPath)) {
                    return fullPath.substring(fullPath.indexOf(inputDir));
                }
            }
        }
        return null;
    }

    private void untarGZFile(String gzHdfsPath, String uncompressedFilePath) throws IOException {
        HdfsUtils.uncompressGZFileWithinHDFS(yarnConfiguration, gzHdfsPath, uncompressedFilePath);
    }

    private void convertCsvToAvro(CsvToAvroFieldMapping fieldTypeMapping, String uncompressedFilePath,
            String avroDirPath) throws IOException {
        simpleCascadingExecutor.transformCsvToAvro(fieldTypeMapping, uncompressedFilePath, avroDirPath);
    }

    @Override
    Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    @Override
    String getCascadingPlatform() {
        return cascadingPlatform;
    }

    public void setFieldTypeMapping(CsvToAvroFieldMapping fieldTypeMapping) {
        this.fieldTypeMapping = fieldTypeMapping;
    }

}
