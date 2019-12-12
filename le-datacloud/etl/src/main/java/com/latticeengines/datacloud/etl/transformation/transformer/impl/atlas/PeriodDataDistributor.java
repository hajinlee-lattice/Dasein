package com.latticeengines.datacloud.etl.transformation.transformer.impl.atlas;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.PERIOD_DATA_DISTRIBUTOR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.PeriodDataDistributorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.util.TimeSeriesDistributer;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;

@Component(PeriodDataDistributor.TRANSFORMER_NAME)
public class PeriodDataDistributor
        extends AbstractTransformer<PeriodDataDistributorConfig> {
    private static final Logger log = LoggerFactory.getLogger(PeriodDataDistributor.class);
    public static final String TRANSFORMER_NAME = PERIOD_DATA_DISTRIBUTOR;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${datacloud.etl.period.distributer.legacy}")
    private boolean useLegacy;

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        PeriodDataDistributorConfig config = getConfiguration(step.getConfig());

        // PeriodId table
        int periodIdx = config.getPeriodIdx() == null ? 0 : config.getPeriodIdx();
        // Source table to distribute by PeriodId
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
            distributeSinglePeriodStore(config, step, periodDir, inputDir);
        } else {
            distributeMultiPeriodStore(config, step, periodDir, inputDir);
        }

        step.setTarget(null);
        step.setCount(0L);
        return true;
    }

    /**
     * For daily store which doesn't have different PeriodName
     *
     * @param config
     * @param step
     * @param periodDir
     * @param inputDir
     */
    private void distributeSinglePeriodStore(PeriodDataDistributorConfig config, TransformStep step,
            String periodDir, String inputDir) {
        // Target table to distribute to
        int targetIdx = config.getTransactinIdx() == null ? 2 : config.getTransactinIdx();
        String targetDir = getSourceHdfsDir(step, targetIdx);

        Set<Integer> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir, config.getPeriodField());
        log.info("Period to cleanup and distribute: {}",
                String.join(",", periods.stream().map(String::valueOf).collect(Collectors.toList())));

        if (useLegacy) {
            if (config.isRetryable()) {
                TimeSeriesUtils.distributePeriodDataWithRetry(yarnConfiguration, inputDir, targetDir, periods,
                        config.getPeriodField());
            } else {
                // M30 temporary solution to avoid PA failure as regression: if
                // retry is disabled, intermittent HDFS write failure is ignored
                TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, targetDir, periods,
                        config.getPeriodField(), true);
            }
        } else {
            @SuppressWarnings("serial")
            TimeSeriesDistributer distributer = new TimeSeriesDistributer.DistributerBuilder() //
                    .yarnConfig(yarnConfiguration) //
                    .inputDir(inputDir) //
                    .targetDirs(new HashMap<String, String>() {
                        {
                            put(TimeSeriesDistributer.DUMMY_PERIOD, targetDir);
                        }
                    }) //
                    .periods(new HashMap<String, Set<Integer>>() {
                        {
                            put(TimeSeriesDistributer.DUMMY_PERIOD, periods);
                        }
                    }) //
                    .periodField(config.getPeriodField()) //
                    .periodNameField(null) //
                    .build();
            distributer.distributePeriodData();
        }
    }

    /**
     * For multi-period store, eg. WeekStore, MonthStore, QuarterStore &
     * YearStore
     *
     * @param config
     * @param step
     * @param periodDir
     * @param inputDir
     */
    private void distributeMultiPeriodStore(PeriodDataDistributorConfig config, TransformStep step, String periodDir,
            String inputDir) {
        if (MapUtils.isEmpty(config.getTransactionIdxes())) {
            throw new RuntimeException("In MultiPeriod mode, please provide PeriodName to TransactionIdx mapping");
        }
        // PeriodName -> [PeriodIds]
        Map<String, Set<Integer>> periods = TimeSeriesUtils.collectPeriods(yarnConfiguration, periodDir,
                config.getPeriodField(), config.getPeriodNameField());
        for (Map.Entry<String, Set<Integer>> period : periods.entrySet()) {
            log.info("For {} period store, period to cleanup and distribute: {}", period.getKey(),
                    String.join(",", period.getValue().stream().map(String::valueOf).collect(Collectors.toList())));
        }
        // PeriodName -> TargetDir
        Map<String, String> targetDirs = new HashMap<>();
        for (Map.Entry<String, Integer> ent : config.getTransactionIdxes().entrySet()) {
            String periodName = ent.getKey();
            String targetDir = getSourceHdfsDir(step, config.getTransactionIdxes().get(periodName));
            targetDirs.put(periodName, targetDir);
        }

        if (useLegacy) {
            if (config.isRetryable()) {
                TimeSeriesUtils.distributePeriodDataWithRetry(yarnConfiguration, inputDir, targetDirs, periods,
                        config.getPeriodField(), config.getPeriodNameField());
            } else {
                // M30 temporary solution to avoid PA failure as regression: if
                // retry is disabled, intermittent HDFS write failure is ignored
                TimeSeriesUtils.distributePeriodData(yarnConfiguration, inputDir, targetDirs, periods,
                        config.getPeriodField(), config.getPeriodNameField(), true);
            }
        } else {
            TimeSeriesDistributer distributer = new TimeSeriesDistributer.DistributerBuilder() //
                    .yarnConfig(yarnConfiguration) //
                    .inputDir(inputDir) //
                    .targetDirs(targetDirs) //
                    .periods(periods) //
                    .periodField(config.getPeriodField()) //
                    .periodNameField(config.getPeriodNameField()) //
                    .build();
            distributer.distributePeriodData();
        }
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected  Class<? extends TransformerConfig> getConfigurationClass() {
        return PeriodDataDistributorConfig.class;
    }

    @Override
    protected boolean validateConfig(PeriodDataDistributorConfig config, List<String> sourceNames) {
        return true;
    }

}
