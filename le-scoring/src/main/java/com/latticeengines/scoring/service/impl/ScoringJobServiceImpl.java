package com.latticeengines.scoring.service.impl;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.util.ScoringJobUtil;

@Component("scoringJobServiceImpl")
public class ScoringJobServiceImpl implements ScoringJobService {

    @Autowired
    private JobService jobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${scoring.mapper.max.input.split.size}")
    private String maxInputSplitSize;

    @Value("${scoring.mapper.min.input.split.size}")
    private String minInputSplitSize;

    @Value("${scoring.mapper.threshold}")
    private String recordFileThreshold;

    @Value("${scoring.mapper.logdir}")
    private String scoringMapperLogDir;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    @Override
    public ApplicationId score(Properties properties) {
        return jobService.submitMRJob(ScoringDaemonService.SCORING_JOB_TYPE, properties);
    }

    @Override
    public ApplicationId score(ScoringConfiguration scoringConfig) {
        Properties properties = generateCustomizedProperties(scoringConfig);
        return this.score(properties);
    }

    private Properties generateCustomizedProperties(ScoringConfiguration scoringConfig) {
        String tenant = CustomerSpace.parse(scoringConfig.getCustomer()).toString();

        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), tenant);
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getScoringQueueNameForSubmission());
        properties.setProperty(MapReduceProperty.INPUT.name(), scoringConfig.getSourceDataDir());
        properties.setProperty(MapReduceProperty.OUTPUT.name(), scoringConfig.getTargetResultDir());
        properties.setProperty(MapReduceProperty.MAX_INPUT_SPLIT_SIZE.name(), maxInputSplitSize);
        properties.setProperty(MapReduceProperty.MIN_INPUT_SPLIT_SIZE.name(), minInputSplitSize);
        properties.setProperty(ScoringProperty.RECORD_FILE_THRESHOLD.name(), recordFileThreshold);
        properties.setProperty(ScoringProperty.UNIQUE_KEY_COLUMN.name(), scoringConfig.getUniqueKeyColumn());
        properties.setProperty(ScoringProperty.TENANT_ID.name(), tenant);
        properties.setProperty(ScoringProperty.LOG_DIR.name(), scoringMapperLogDir);
        properties.setProperty(ScoringProperty.MODEL_GUID.name(), commaJoiner.join(scoringConfig.getModelGuids()));
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), String.valueOf(Long.MIN_VALUE));
        List<String> modelUrls = ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant,
                customerBaseDir, scoringConfig.getModelGuids());
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(modelUrls));
        return properties;
    }
}
