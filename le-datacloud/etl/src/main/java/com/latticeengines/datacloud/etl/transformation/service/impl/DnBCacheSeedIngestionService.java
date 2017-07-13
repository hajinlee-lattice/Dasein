package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.DataImportedFromHDFS;
import com.latticeengines.datacloud.core.source.impl.DnBCacheSeedRaw;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DnBCacheSeedInputSourceConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DnBCacheSeedRawConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("dnbCacheSeedIngestionService")
public class DnBCacheSeedIngestionService extends AbstractFirehoseTransformationService<DnBCacheSeedRawConfiguration>
        implements TransformationService<DnBCacheSeedRawConfiguration> {
    private static final String DATA_FLOW_BEAN_NAME = "dnbCacheSeedUncompressAndConvertToAvroFlow";

    private static final Logger log = LoggerFactory.getLogger(DnBCacheSeedIngestionService.class);

    private static final String SCHEMA = "schema";
    private static final String DNB_CACHESEED_AVRO_SCHEMA_AVSC = "DnBCacheSeedAvroSchema.avsc";

    @Autowired
    private DnBCacheSeedRaw source;

    @Autowired
    private FirehoseTransformationDataFlowService transformationDataFlowService;

    @Override
    public DataImportedFromHDFS getSource() {
        return source;
    }

    @Override
    Logger getLogger() {
        return log;
    }

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    @Override
    protected void executeDataFlow(TransformationProgress progress, String workflowDir,
            DnBCacheSeedRawConfiguration transformationConfiguration) {
        CsvToAvroFieldMappingImpl fieldTypeMapping = new CsvToAvroFieldMappingImpl(
                transformationConfiguration.getSourceColumns());
        transformationDataFlowService.setFieldTypeMapping(fieldTypeMapping);
        transformationDataFlowService.executeDataProcessing(source, workflowDir, getVersion(progress),
                progress.getRootOperationUID(), DATA_FLOW_BEAN_NAME, transformationConfiguration);
    }

    @Override
    Date checkTransformationConfigurationValidity(DnBCacheSeedRawConfiguration conf) {
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    protected DnBCacheSeedRawConfiguration createNewConfiguration(List<String> latestBaseVersion,
            String newLatestVersion, List<SourceColumn> sourceColumns) {
        DnBCacheSeedRawConfiguration configuration = new DnBCacheSeedRawConfiguration();
        configuration.setInputFirehoseVersion(latestBaseVersion.get(0));
        DnBCacheSeedInputSourceConfig inputSourceConfig = new DnBCacheSeedInputSourceConfig();
        inputSourceConfig.setVersion(latestBaseVersion.get(0));
        configuration.setDnbCacheSeedInputSourceConfig(inputSourceConfig);
        setAdditionalDetails(newLatestVersion, sourceColumns, configuration);
        return configuration;
    }

    @Override
    void uploadSourceSchema(String workflowDir) throws IOException {
        String schemaFileName = DNB_CACHESEED_AVRO_SCHEMA_AVSC;
        InputStream fileStream = ClassLoader.getSystemResourceAsStream(SCHEMA + HDFS_PATH_SEPARATOR + schemaFileName);
        String targetPath = workflowDir + HDFS_PATH_SEPARATOR + schemaFileName;

        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
    }

    @Override
    DnBCacheSeedRawConfiguration parseTransConfJsonInsideWorkflow(String confStr) throws IOException {
        return JsonUtils.deserialize(confStr, DnBCacheSeedRawConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return DnBCacheSeedRawConfiguration.class;
    }

}
