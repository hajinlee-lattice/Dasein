package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.PeriodDataCleaner.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_CLEANER;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataCleanerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(TRANSFORMER_NAME)
public class PeriodDataCleaner
        extends AbstractTransformer<PeriodDataCleanerConfig> {
    private static final Logger log = LoggerFactory.getLogger(PeriodDataCleaner.class);
    public static final String TRANSFORMER_NAME = PERIOD_DATA_CLEANER;
    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        PeriodDataCleanerConfig config = getConfiguration(step.getConfig());

        // Source to extract PeriodId
        // For transaction, it's transaction raw store or raw diff
        String periodDir = getSourceHdfsDir(step, 0);
        if (StringUtils.isBlank(config.getPeriodField())) {
            config.setPeriodField(InterfaceName.PeriodId.name());
        }
        if (StringUtils.isBlank(config.getPeriodNameField())) {
            config.setPeriodNameField(InterfaceName.PeriodName.name());
        }

        // Clean daily store
        if (!config.isMultiPeriod()) {
            // Target daily store to clean
            String periodStoreDir = getSourceHdfsDir(step, 1);

            Set<Integer> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField());

            for (Integer period : periods) {
                log.info("Period to clean " + period);
            }

            TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, periodStoreDir, periods);
        } else { // Clean period store
            if (MapUtils.isEmpty(config.getTransactionIdxes())) {
                throw new RuntimeException(
                        "Please provide period name to transaction idx mapping for multi-period mode");
            }
            Map<String, String> periodStoreDirs = new HashMap<>();
            for (Map.Entry<String, Integer> ent : config.getTransactionIdxes().entrySet()) {
                periodStoreDirs.put(ent.getKey(), getSourceHdfsDir(step, ent.getValue()));
            }
            Map<String, Set<Integer>> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField(), config.getPeriodNameField());
            for (String periodName : periods.keySet()) {
                TimeSeriesUtils.cleanupPeriodData(yarnConfiguration, periodStoreDirs.get(periodName),
                        periods.get(periodName));
            }
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
        return PeriodDataCleanerConfig.class;
    }


    @Override
    protected boolean validateConfig(PeriodDataCleanerConfig config, List<String> sourceNames) {
        return true;
    }

}
