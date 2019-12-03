package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.PeriodDataFilter.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_FILTER;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataFilterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(TRANSFORMER_NAME)
public class PeriodDataFilter
        extends AbstractTransformer<PeriodDataFilterConfig> {
    public static final String TRANSFORMER_NAME = PERIOD_DATA_FILTER;
    private static final Logger log = LoggerFactory.getLogger(PeriodDataFilter.class);

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        PeriodDataFilterConfig config = getConfiguration(step.getConfig());

        String periodDir = getSourceHdfsDir(step, 0);
        String transactionDir = getSourceHdfsDir(step, 1);

        if (!config.isMultiPeriod()) {
            Set<Integer> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField());

            for (Integer period : periods) {
                log.info("Period to filter " + period);
            }

            TimeSeriesUtils.collectPeriodData(yarnConfiguration, workflowDir, transactionDir, periods,
                    config.getEarliestTransactionDate());
        } else {
            if (CollectionUtils.isEmpty(config.getPeriodStrategies())) {
                throw new RuntimeException("Please provide period strategies for multi-period mode");
            }
            Map<String, Set<Integer>> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                    config.getPeriodField(), config.getPeriodNameField());
            TimeSeriesUtils.collectPeriodData(yarnConfiguration, workflowDir, transactionDir, periods,
                    config.getPeriodStrategies(), config.getEarliestTransactionDate());
        }

        return true;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }


    @Override
    protected  Class<? extends TransformerConfig> getConfigurationClass() {
        return PeriodDataFilterConfig.class;
    }

    @Override
    protected boolean validateConfig(PeriodDataFilterConfig config, List<String> sourceNames) {
        return true;
    }
}
