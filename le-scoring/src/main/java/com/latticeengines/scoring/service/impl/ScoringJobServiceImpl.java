package com.latticeengines.scoring.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {

    @Autowired
    private JobService jobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

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

    @Value("${dataplatform.hdfs.stack:}")
    protected String stackName;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    @Override
    public ApplicationId score(Properties properties) {
        return jobService.submitMRJob(ScoringDaemonService.SCORING_JOB_TYPE, properties);
    }

    @Override
    public ApplicationId score(ScoringConfiguration scoringConfig) {
        validateScoringConfig(scoringConfig);
        Properties properties = generateCustomizedProperties(scoringConfig);
        return this.score(properties);
    }

    private void validateScoringConfig(ScoringConfiguration scoringConfig) {
        if (StringUtils.isBlank(scoringConfig.getCustomer())) {
            throw new LedpException(LedpCode.LEDP_20022);
        }

        try {
            if (StringUtils.isBlank(scoringConfig.getSourceDataDir())
                    || !HdfsUtils.fileExists(yarnConfiguration, scoringConfig.getSourceDataDir())) {
                throw new LedpException(LedpCode.LEDP_20023);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_20023, e);
        }
        try {
            if (StringUtils.isBlank(scoringConfig.getTargetResultDir())
                    || HdfsUtils.fileExists(yarnConfiguration, scoringConfig.getTargetResultDir())) {
                throw new LedpException(LedpCode.LEDP_20024);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_20024, e);
        }
        if (StringUtils.isBlank(scoringConfig.getUniqueKeyColumn())) {
            throw new LedpException(LedpCode.LEDP_20025);
        }
        if (CollectionUtils.isEmpty(scoringConfig.getModelGuids())) {
            throw new LedpException(LedpCode.LEDP_20026);
        }
    }

    public Properties generateCustomizedProperties(ScoringConfiguration scoringConfig) {
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

        List<String> cacheFiles = new ArrayList<>();
        try {
            cacheFiles = ScoringJobUtil.getCacheFiles(yarnConfiguration,
                    versionManager.getCurrentVersionInStack(stackName));
            cacheFiles.addAll(ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, customerBaseDir,
                    scoringConfig.getModelGuids(), Boolean.TRUE.booleanValue()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(cacheFiles));
        properties.setProperty(ScoringProperty.USE_SCOREDERIVATION.name(), Boolean.TRUE.toString());
        return properties;
    }

    public void setConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }
}
