package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.PeriodDataDistributor.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_DISTRIBUTOR;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(TRANSFORMER_NAME)
public class PeriodDataDistributor
        extends AbstractTransformer<PeriodDataDistributorConfig> {
    private static final Logger log = LoggerFactory.getLogger(PeriodDataDistributor.class);
    public static final String TRANSFORMER_NAME = PERIOD_DATA_DISTRIBUTOR;

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        PeriodDataDistributorConfig config = getConfiguration(step.getConfig());

        String periodDir = getSourceHdfsDir(step, 0);
        String inputDir = getSourceHdfsDir(step, 1);
        String transactionDir = getSourceHdfsDir(step, 2);

        Set<Integer> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir, config.getPeriodField());
        for (Integer period : periods) {
            log.info("Period to distribute " + period);
        }
        TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, transactionDir, periods, config.getPeriodField());
        try {
            List<String> avroFiles = HdfsUtils.getFilesForDir(yarnConfiguration, periodDir, ".*.avro$");
            for (String fileName : avroFiles) {
                HdfsUtils.copyFiles(yarnConfiguration, fileName, workflowDir);
            }
        } catch (Exception e) {
            log.error("Failed to copy file from " + periodDir + " to " + workflowDir, e);
            return false;
        }
        return true;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected  Class<? extends TransformerConfig> getConfigurationClass() {
        return PeriodDataDistributorConfig.class;
    }

    protected boolean validateConfig(PeriodDataDistributorConfig config, List<String> sourceNames) {
        return true;
    }

}
