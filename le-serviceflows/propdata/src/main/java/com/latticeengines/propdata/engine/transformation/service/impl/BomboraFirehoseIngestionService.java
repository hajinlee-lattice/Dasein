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

    private static final String VERSION = "VERSION";

    private static final Log log = LogFactory.getLog(BomboraFirehoseIngestionService.class);

    @Autowired
    private TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private BomboraFirehose source;

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
    protected void executeDataFlow(TransformationProgress progress, String workflowDir) {
        transformationDataFlowService.executeDataProcessing(source, workflowDir, getVersion(progress),
                progress.getRootOperationUID(), "bomboraUntarAndConvertToAvroFlow");
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
        String schemaFileName = "BomboraFirehoseAvroSchema.avsc";
        InputStream fileStream = ClassLoader.getSystemResourceAsStream("schema/" + schemaFileName);
        String targetPath = workflowDir + "/" + schemaFileName;
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, fileStream, targetPath);
    }

    @Override
    TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException {
        return om.readValue(confStr, BomboraFirehoseConfiguration.class);
    }
}
