package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_AM_REFRESH_VER_UPDATER;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(AMRefreshVersionUpdater.TRANSFORMER_NAME)
public class AMRefreshVersionUpdater extends AbstractTransformer<TransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(AMRefreshVersionUpdater.class);
    public static final String TRANSFORMER_NAME = TRANSFORMER_AM_REFRESH_VER_UPDATER;
    GeneralSource baseSource = new GeneralSource("AMRefreshVersionUpdater");
    GeneralSource targetSource = new GeneralSource("LDCDEV_AMRefreshVersionUpdater");

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private DataCloudVersionService dataCloudVersionService;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(TransformerConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            String avroName = getSourceHdfsDir(step, 0);
            String targetAvroName = getTargetHdfsDir(step);
            try {
                List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, avroName,
                        ".*.avro$");
                if (HdfsUtils.fileExists(yarnConfiguration, targetAvroName)) {
                    List<String> successFile = HdfsUtils.getFilesForDir(yarnConfiguration,
                            targetAvroName, "_SUCCESS");
                    if (successFile.size() > 0) {
                        FileSystem fs = FileSystem.get(new Configuration());
                        fs.delete(new Path(successFile.get(0)), true);
                    }
                }
                HdfsUtils.copyFiles(yarnConfiguration, avroFiles.get(0), workflowDir);
            } catch (Exception e) {
                log.error("Failed to copy file from " + avroName + " to " + workflowDir, e);
                return false;
            }
            dataCloudVersionService.updateRefreshVersion();
        } catch (Exception e) {
            log.error("Failed to update refresh version", e);
            return false;
        }
        return true;
    }

}
