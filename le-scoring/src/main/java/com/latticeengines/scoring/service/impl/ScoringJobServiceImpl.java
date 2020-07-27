package com.latticeengines.scoring.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.LedpQueueAssigner;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.util.ScoringConstants;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {

    private static final Logger log = LoggerFactory.getLogger(ScoringJobServiceImpl.class);

    @Inject
    private JobService jobService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

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

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${dataplatform.python.conda.env}")
    private String condaEnv;

    @Value("${dataplatform.python2.conda.env}")
    private String condaEnvP2;

    @Value("${scoring.mapreduce.memory}")
    private int memory;

    @Value("${scoring.mapreduce.tasks.maximum}")
    private int maxTasks;

    @Inject
    private ManifestService manifestService;

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    @Override
    public ApplicationId score(Properties properties) {
        if (StringUtils.isBlank(properties.getProperty(ScoringProperty.CONDA_ENV.name()))) {
            properties.setProperty(ScoringProperty.CONDA_ENV.name(), condaEnv);
            log.info("Conda env was not specified, set it to the default value {}", condaEnv);
        } else {
            log.info("Using specified conda env {}", properties.getProperty(ScoringProperty.CONDA_ENV.name()));
        }
        if (StringUtils.isBlank(properties.getProperty(ScoringProperty.CONDA_ENV_P2.name()))) {
            properties.setProperty(ScoringProperty.CONDA_ENV_P2.name(), condaEnvP2);
            log.info("Python 2 conda env was not specified, set it to the default value {}", condaEnv);
        } else {
            log.info("Using specified python 2 conda env {}", properties.getProperty(ScoringProperty.CONDA_ENV.name()));
        }
        return jobService.submitMRJob(ScoringConstants.SCORING_JOB_TYPE, properties);
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
        if (CollectionUtils.isEmpty(scoringConfig.getModelGuids()) && CollectionUtils.isEmpty(scoringConfig.getP2ModelGuids())) {
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
        properties.setProperty(ScoringProperty.MODEL_GUID.name(), //
                StringUtils.join(scoringConfig.getModelGuids(), ","));
        properties.setProperty(ScoringProperty.MODEL_GUID_P2.name(), //
                StringUtils.join(scoringConfig.getP2ModelGuids(), ","));
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), String.valueOf(Long.MIN_VALUE));
        properties.setProperty(ScoringProperty.SCORE_INPUT_TYPE.name(), scoringConfig.getScoreInputType().name());
        properties.setProperty(ScoringProperty.READ_MODEL_ID_FROM_RECORD.name(),
                String.valueOf(scoringConfig.readModelIdFromRecord()));
        properties.setProperty(ScoringProperty.CONDA_ENV_P2.name(), condaEnvP2);
        properties.setProperty(ScoringProperty.CONDA_ENV.name(), condaEnv);

        List<String> cacheFiles;
        try {
            syncModelsFromS3ToHdfs(tenant);
            cacheFiles = ScoringJobUtil.getCacheFiles(yarnConfiguration, manifestService.getLedpStackVersion(), //
                    manifestService.getLedsVersion());
            List<String> allGuids = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(scoringConfig.getModelGuids())) {
                allGuids.addAll(scoringConfig.getModelGuids());
            }
            if (CollectionUtils.isNotEmpty(scoringConfig.getP2ModelGuids())) {
                allGuids.addAll(scoringConfig.getP2ModelGuids());
            }
            cacheFiles.addAll(ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, customerBaseDir,
                    allGuids, Boolean.TRUE));
            log.info("cacheFiles={}", cacheFiles);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(cacheFiles));
        if (Boolean.FALSE.equals(scoringConfig.getUseScorederivation())) {
            properties.setProperty(ScoringProperty.USE_SCOREDERIVATION.name(), Boolean.FALSE.toString());
        } else {
            properties.setProperty(ScoringProperty.USE_SCOREDERIVATION.name(), Boolean.TRUE.toString());
        }

        setContainerProperties(properties);
        return properties;
    }

    public void setConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    private void setContainerProperties(Properties properties) {
        String memSize = memory + "";
        log.info(String.format("Setting container mem size for %s: %s", "Scoring", memSize));
        properties.put("mapreduce.map.memory.mb", memSize);
        properties.put("mapreduce.reduce.memory.mb", memSize);
        properties.put("mapreduce.tasktracker.map.tasks.maximum", "" + maxTasks);
        properties.put("mapreduce.tasktracker.reduce.tasks.maximum", "" + maxTasks);
    }

    public void syncModelsFromS3ToHdfs(String tenant) {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String customer = CustomerSpace.parse(tenant).toString();
        String tenantId = CustomerSpace.parse(tenant).getTenantId();
        String s3ModelsDir = pathBuilder.getS3AnalyticsModelDir(s3Bucket, tenantId);
        String hdfsModelDir = pathBuilder.getHdfsAnalyticsModelDir(customer);
        final String s3Prefix = s3ModelsDir.replace("s3n://" + s3Bucket + "/", "");
        List<S3ObjectSummary> summaries = s3Service.listObjects(s3Bucket, s3Prefix);
        log.info("Found " + CollectionUtils.size(summaries) + " objects in " + s3Prefix);
        summaries.forEach(summary -> {
            String key = summary.getKey();
            if (key.endsWith("model.json")) {
                log.info("S3 Object: " + summary.getKey());
                String relativePath = key.replace(s3Prefix, "");
                String hdfsPath = hdfsModelDir + relativePath;
                log.info("Hdfs Path: " + hdfsPath);
                try {
                    if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                        InputStream is = s3Service.readObjectAsStream(s3Bucket, key);
                        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath);
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to copy from " + key + " to " + hdfsPath);
                }
            }
        });
    }

}
