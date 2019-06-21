package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_DISTRIBUTOR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(PeriodDataDistributor.TRANSFORMER_NAME)
public class PeriodDataDistributor
        extends AbstractTransformer<PeriodDataDistributorConfig> {
    private static final Logger log = LoggerFactory.getLogger(PeriodDataDistributor.class);
    public static final String TRANSFORMER_NAME = PERIOD_DATA_DISTRIBUTOR;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        PeriodDataDistributorConfig config = getConfiguration(step.getConfig());

        int periodIdx = config.getPeriodIdx() == null ? 0 : config.getPeriodIdx();
        int inputIdx = config.getInputIdx() == null ? 1 : config.getInputIdx();
        String periodDir = getSourceHdfsDir(step, periodIdx);
        String inputDir = getSourceHdfsDir(step, inputIdx);

        if (StringUtils.isBlank(config.getPeriodField())) {
            config.setPeriodField(InterfaceName.PeriodId.name());
        }
        if (StringUtils.isBlank(config.getPeriodNameField())) {
            config.setPeriodNameField(InterfaceName.PeriodName.name());
        }

        if (!config.isMultiPeriod()) {
            int transactionIdx = config.getTransactinIdx() == null ? 2 : config.getTransactinIdx();
            String transactionDir = getSourceHdfsDir(step, transactionIdx);

            Set<Integer> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField());
            for (Integer period : periods) {
                log.debug("Period to distribute " + period);
            }
            TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, transactionDir, periods,
                    config.getPeriodField());
        } else {
            if (MapUtils.isEmpty(config.getTransactionIdxes())) {
                throw new RuntimeException("In MultiPeriod mode, please provide PeriodName to TransactionIdx mapping");
            }
            Map<String, Set<Integer>> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField(), config.getPeriodNameField());
            Map<String, String> targetDirs = new HashMap<>(); // PeriodName -> TransactionDir
            for (Map.Entry<String, Integer> ent : config.getTransactionIdxes().entrySet()) {
                String periodName = ent.getKey();
                String transactionDir = getSourceHdfsDir(step, config.getTransactionIdxes().get(periodName));
                targetDirs.put(periodName, transactionDir);
            }
            TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, targetDirs, periods,
                    config.getPeriodField(), config.getPeriodNameField());
        }

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
