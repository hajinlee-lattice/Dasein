package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
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
import com.latticeengines.domain.exposed.datacloud.dataflow.IngestedFileToSourceParameters;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("ingestedFileToSourceDataFlowService")
public class IngestedFileToSourceDataFlowService extends AbstractTransformationDataFlowService {
    private static final Log log = LogFactory.getLog(IngestedFileToSourceDataFlowService.class);

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private SimpleCascadingExecutor simpleCascadingExecutor;

    public void executeDataFlow(Source source, String workflowDir, String baseVersion,
            IngestedFileToSourceParameters parameters) {
        CsvToAvroFieldMappingImpl fieldTypeMapping = new CsvToAvroFieldMappingImpl(parameters.getColumns());
        String ingestionDir = hdfsPathBuilder.constructIngestionDir(parameters.getIngetionName(), baseVersion)
                .toString();
        log.info("Ingestion Directory: " + ingestionDir);
        
        try {
            if (parameters.getFileName() != null) {
                String filePath = scanDir(ingestionDir,
                        parameters.getFileName() != null ? parameters.getFileName() : parameters.getExtension(), true,
                        true).get(0);
                convertCsvToAvro(fieldTypeMapping, filePath, workflowDir, parameters);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25012, source.getSourceName(), e);
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
            files = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, inputDir, filter, returnFirstMatch);
        } else {
            files = HdfsUtils.getFilesForDir(yarnConfiguration, inputDir, filter);
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
