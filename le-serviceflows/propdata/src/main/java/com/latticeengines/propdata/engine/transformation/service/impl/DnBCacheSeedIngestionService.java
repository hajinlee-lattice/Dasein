package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.impl.DnBCacheSeed;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.DnBCacheSeedConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.DnBCacheSeedInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("dnbCacheSeedIngestionService")
public class DnBCacheSeedIngestionService extends AbstractFirehoseTransformationService
        implements TransformationService {
    private static final String DATA_FLOW_BEAN_NAME = "dnbCacheSeedUncompressAndConvertToAvroFlow";

    private static final Log log = LogFactory.getLog(DnBCacheSeedIngestionService.class);

    private static final String PATH_SEPARATOR = "/";

    private static final String SCHEMA = "schema";

    private static final String DNB_CACHESEED_AVRO_SCHEMA_AVSC = "DnBCacheSeedAvroSchema.avsc";

    private static final String VERSION = "VERSION";

    @Autowired
    private TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private DnBCacheSeed source;

    @Autowired
    private FirehoseTransformationDataFlowService transformationDataFlowService;

    @Override
    public DataImportedFromHDFS getSource() {
        return source;
    }

    @Override
    TransformationProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Log getLogger() {
        return log;
    }

    @Override
    protected TransformationDataFlowService getTransformationDataFlowService() {
        return transformationDataFlowService;
    }

    @Override
    protected void executeDataFlow(TransformationProgress progress, String workflowDir,
            TransformationConfiguration transformationConfiguration) {
        CsvToAvroFieldMappingImpl fieldTypeMapping = new CsvToAvroFieldMappingImpl(
                transformationConfiguration.getSourceColumns());
        transformationDataFlowService.setFieldTypeMapping(fieldTypeMapping);
        transformationDataFlowService.executeDataProcessing(source, workflowDir,
                getVersion(progress), progress.getRootOperationUID(), DATA_FLOW_BEAN_NAME,
                transformationConfiguration);
    }

    @Override
    Date checkTransformationConfigurationValidity(
            TransformationConfiguration transformationConfiguration) {
        DnBCacheSeedConfiguration conf = (DnBCacheSeedConfiguration) transformationConfiguration;
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    protected TransformationConfiguration createNewConfiguration(List<String> latestBaseVersion,
            String newLatestVersion, List<SourceColumn> sourceColumns) {
        DnBCacheSeedConfiguration configuration = new DnBCacheSeedConfiguration();
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
        InputStream fileStream = ClassLoader
                .getSystemResourceAsStream(SCHEMA + PATH_SEPARATOR + schemaFileName);
        String targetPath = workflowDir + PATH_SEPARATOR + schemaFileName;

        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr)
            throws IOException {
        return JsonUtils.deserialize(confStr, DnBCacheSeedConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return DnBCacheSeedConfiguration.class;
    }

}
