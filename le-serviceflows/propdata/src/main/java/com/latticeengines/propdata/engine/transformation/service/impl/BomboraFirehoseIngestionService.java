package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.BomboraFirehoseFieldMapping;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.impl.BomboraFirehose;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component("bomboraFirehoseIngestionService")
public class BomboraFirehoseIngestionService extends AbstractFirehoseTransformationService
        implements TransformationService {

    private static final String DATA_FLOW_BEAN_NAME = "bomboraUntarAndConvertToAvroFlow";

    private static final Log log = LogFactory.getLog(BomboraFirehoseIngestionService.class);

    private static final String PATH_SEPARATOR = "/";

    private static final String SCHEMA = "schema";

    private static final String BOMBORA_FIREHOSE_AVRO_SCHEMA_AVSC = "BomboraFirehoseAvroSchema.avsc";

    private static final String VERSION = "VERSION";

    @Autowired
    private TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private BomboraFirehose source;

    @Autowired
    private FirehoseTransformationDataFlowService transformationDataFlowService;

    @Autowired
    private BomboraFirehoseFieldMapping fieldTypeMapping;

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
    protected void executeDataFlow(TransformationProgress progress, String workflowDir) {
        transformationDataFlowService.setFieldTypeMapping(fieldTypeMapping);
        transformationDataFlowService.executeDataProcessing(source, workflowDir, getVersion(progress),
                progress.getRootOperationUID(), DATA_FLOW_BEAN_NAME);
    }

    @Override
    Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration) {
        BomboraFirehoseConfiguration conf = (BomboraFirehoseConfiguration) transformationConfiguration;
        conf.getSourceConfigurations().put(VERSION, conf.getVersion());
        try {
            return HdfsPathBuilder.dateFormat.parse(conf.getVersion());
        } catch (ParseException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    protected TransformationConfiguration createNewConfiguration(String latestBaseVersion, String newLatestVersion) {
        BomboraFirehoseConfiguration configuration = new BomboraFirehoseConfiguration();
        configuration.setInputFirehoseVersion(latestBaseVersion);
        configuration.setSourceName(source.getSourceName());
        Map<String, String> sourceConfigurations = new HashMap<>();
        configuration.setSourceConfigurations(sourceConfigurations);
        configuration.setVersion(newLatestVersion);
        return configuration;
    }

    @Override
    void uploadSourceSchema(String workflowDir) throws IOException {
        String schemaFileName = BOMBORA_FIREHOSE_AVRO_SCHEMA_AVSC;
        InputStream fileStream = ClassLoader.getSystemResourceAsStream(SCHEMA + PATH_SEPARATOR + schemaFileName);
        String targetPath = workflowDir + PATH_SEPARATOR + schemaFileName;
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException {
        return om.deserialize(confStr, BomboraFirehoseConfiguration.class);
    }

    @Override
    public Class<? extends TransformationConfiguration> getConfigurationClass() {
        return BomboraFirehoseConfiguration.class;
    }
}
