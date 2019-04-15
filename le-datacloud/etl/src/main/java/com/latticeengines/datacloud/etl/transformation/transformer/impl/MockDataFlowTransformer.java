package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(MockDataFlowTransformer.TRANSFORMER_NAME)
public class MockDataFlowTransformer extends AbstractTransformer<TransformerConfig> {

    private static final Logger log = LoggerFactory.getLogger(MockDataFlowTransformer.class);

    public static final String TRANSFORMER_NAME = "MockDataFlowTransformer";

    @Autowired
    protected Configuration yarnConfiguration;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(TransformerConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir,
            TransformStep step) {
        try {
            String avroName = getSourceHdfsDir(step, 0);
            try {
                List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroName,
                        ".*.avro$");
                HdfsUtils.copyFiles(yarnConfiguration, avroFiles.get(0), workflowDir);
            } catch (Exception e) {
                log.error("Failed to copy file from " + avroName + " to " + workflowDir, e);
                return false;
            }
        } catch (Exception e) {
            log.error("Failed to upload", e);
            return false;
        }
        return true;
    }

}
