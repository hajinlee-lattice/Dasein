package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.publication.service.PublishService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.Publication;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;
import com.latticeengines.domain.exposed.datacloud.publication.DynamoDestination;
import com.latticeengines.domain.exposed.datacloud.publication.PublicationConfiguration;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.yarn.exposed.service.JobService;

@Service("dynamoPublishService")
public class DynamoPublishService extends AbstractPublishService
        implements PublishService<PublishToDynamoConfiguration> {

    private static Logger log = LoggerFactory.getLogger(DynamoPublishService.class);

    private static final String PARTITION_KEY = "Id";
    private static final String PREFIX = "_REPO_DataCloud_RECORD_";
    private static final Long THREE_DAY = TimeUnit.DAYS.toSeconds(3);

    static final String TAG_LE_ENV = "le-env";
    static final String TAG_LE_PRODUCT = "le-product";
    static final String TAG_LE_PRODUCT_VALUE = "lpi";

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private JobService jobService;

    private DynamoService overridingDynamoService;

    @PostConstruct
    private void postConstruct() {
        PublishServiceFactory.register(Publication.PublicationType.DYNAMO, this);
    }

    @Override
    public PublicationProgress publish(PublicationProgress progress, PublishToDynamoConfiguration configuration) {
        log.info("Execute publish to dynamo.");
        configuration = configurationParser.parseDynamoAlias(configuration);
        DynamoService dynamoService = getDynamoService(configuration);

        DynamoDestination destination = (DynamoDestination) progress.getDestination();
        log.info("Read destination for this progress: \n" + JsonUtils.pprint(destination));
        configuration.setDestination(destination);

        log.info("Publication Strategy = " + configuration.getPublicationStrategy());
        String tableName = getTableName(configuration);
        log.info("Target table name is " + tableName);
        switch (configuration.getPublicationStrategy()) {
        case REPLACE:
            if (dynamoService.hasTable(tableName)) {
                if (configuration.getAlias() == PublishToDynamoConfiguration.Alias.Production) {
                    throw new RuntimeException(
                            tableName
                                    + " already exists in production env. Please manually delete the table before publish.");
                }
                log.info("Delete table " + tableName + " if exists");
                dynamoService.deleteTable(tableName);
            }
            createTable(dynamoService, tableName, configuration);
            log.info("Table " + tableName + " is created");
            progress = progressService.update(progress).destination(destination).progress(0.3f).commit();
        case APPEND:
            if (PublicationConfiguration.PublicationStrategy.APPEND.equals(configuration.getPublicationStrategy())) {
                long readCapacity = configuration.getLoadingReadCapacity();
                long writeCapacity = configuration.getLoadingWriteCapacity();
                updateThroughput(dynamoService, tableName, readCapacity, writeCapacity);
            }
            String sourceVersion = progress.getSourceVersion();
            String sourceName = progress.getPublication().getSourceName();
            uploadData(dynamoService, tableName, sourceName, sourceVersion, configuration);
            progress = progressService.update(progress).destination(progress.getDestination()).progress(0.9f).commit();
            break;
        default:
            throw new UnsupportedOperationException(configuration.getPublicationStrategy() + " is not supported");
        }
        resumeThroughput(dynamoService, tableName, configuration);

        long count = 0L;
        progress = progressService.update(progress) //
                .progress(1.0f) //
                .rowsPublished(count) //
                .status(ProgressStatus.FINISHED) //
                .commit();
        return progress;
    }

    private String getTableName(PublishToDynamoConfiguration configuration) {
        String recordType = configuration.getRecordType();
        DynamoDestination destination = (DynamoDestination) configuration.getDestination();
        String version = destination.getVersion();
        recordType = String.format("%s%s", recordType, version);
        return convertToFabricStoreName(recordType);
    }

    private void createTable(DynamoService dynamoService, String tableName, PublishToDynamoConfiguration configuration) {
        log.info("Creating dynamo table " + tableName);
        long readCapacity = configuration.getLoadingReadCapacity();
        long writeCapacity = configuration.getLoadingWriteCapacity();
        dynamoService.createTable(tableName, readCapacity, writeCapacity, PARTITION_KEY, ScalarAttributeType.S.name(),
                null, null);
        log.info("Created dynamo table " + tableName);
        String environment = "dev";
        switch (configuration.getAlias()) {
            case QA:
                environment = "qa";
                break;
            case Production:
                environment = "production";
                break;
        }
        Map<String, String> tags = ImmutableMap.of(TAG_LE_ENV, environment, TAG_LE_PRODUCT, TAG_LE_PRODUCT_VALUE);
        dynamoService.tagTable(tableName, tags);
        log.info("Tagged dynamo table " + tableName + ": " + JsonUtils.serialize(tags));
    }

    private void uploadData(DynamoService dynamoService, String tableName, String sourceName, String sourceVersion,
            PublishToDynamoConfiguration configuration) {
        log.info("Uploading data to dynamo table " + tableName);
        HdfsToDynamoConfiguration eaiConfig = generateEaiConfig(tableName, sourceName, sourceVersion, configuration);
        AppSubmission appSubmission = eaiProxy.submitEaiJob(eaiConfig);
        String appId = appSubmission.getApplicationIds().get(0);
        JobStatus jobStatus = jobService.waitFinalJobStatus(appId, THREE_DAY.intValue());
        if (!FinalApplicationStatus.SUCCEEDED.equals(jobStatus.getStatus())) {
            resumeThroughput(dynamoService, tableName, configuration);
            throw new RuntimeException("Yarn application " + appId + " did not finish in SUCCEEDED status, but " //
                    + jobStatus.getStatus() + " instead.");
        }
        log.info("Uploaded data to dynamo table " + tableName);
    }

    private void resumeThroughput(DynamoService dynamoService, String tableName, PublishToDynamoConfiguration configuration) {
        long readCapacity = configuration.getRuntimeReadCapacity();
        long writeCapacity = configuration.getRuntimeWriteCapacity();
        updateThroughput(dynamoService, tableName, readCapacity, writeCapacity);
    }

    /**
     * Update read & write capacity of Dynamo table. Retry 3 times, waiting
     * intervals are 30s, 60s, 120s. If the table capacity is on demand, skip
     * updating
     *
     * Reason: If Dynamo table is configured as auto-scaling and it's already
     * under scaling status, request of updating capacity will fail
     *
     * @param dynamoService
     * @param tableName
     * @param readCapacity
     * @param writeCapacity
     */
    private void updateThroughput(DynamoService dynamoService, String tableName, long readCapacity,
            long writeCapacity) {
        if (dynamoService.isCapacityOnDemand(tableName)) {
            log.warn("Capacity for table {} is on demand. Skip updating throughput.", tableName);
            return;
        }
        RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate(3, 30, 2, null);
        try {
            retry.execute(context -> {
                if (context.getRetryCount() >= 1) {
                    log.warn("Updating capacity of table {} wasn't successful. Retrying for {} times", tableName,
                            context.getRetryCount());
                }
                dynamoService.updateTableThroughput(tableName, readCapacity, writeCapacity);
                return null;
            });
        } catch (Exception ex) {
            throw ex;
        }
        log.info("Changed throughput of {} to ({}, {})", tableName, readCapacity, writeCapacity);
    }

    private String entityClassCanonicalName(PublishToDynamoConfiguration configuration) {
        try {
            Class<?> clz = Class.forName(configuration.getEntityClass());
            return clz.getCanonicalName();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot parse record class " + configuration.getEntityClass(), e);
        }
    }

    private HdfsToDynamoConfiguration generateEaiConfig(String tableName, String sourceName, String sourceVersion,
            PublishToDynamoConfiguration publishConfig) {
        String recordType = tableName.replace(PREFIX, "");

        HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();
        eaiConfig.setName("DataCloud_Dynamo_" + recordType);
        eaiConfig.setCustomerSpace(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE));
        eaiConfig.setExportDestination(ExportDestination.DYNAMO);
        eaiConfig.setExportFormat(ExportFormat.AVRO);
        eaiConfig.setExportInputPath(getExportInputPath(sourceName, sourceVersion, publishConfig));
        eaiConfig.setUsingDisplayName(false);
        eaiConfig.setExportTargetPath("/tmp/path");

        Map<String, String> properties = new HashMap<>();
        String recordClass = entityClassCanonicalName(publishConfig);
        log.info("Resolved entity class : " + recordClass);
        properties.put("eai.export.aws.access.key.id", publishConfig.getAwsAccessKeyEncrypted());
        properties.put("eai.export.aws.secret.key", publishConfig.getAwsSecretKeyEncrypted());
        properties.put("eai.export.dynamo.entity.class", recordClass);
        properties.put("eai.export.dynamo.repository", "DataCloud");
        properties.put("eai.export.dynamo.record.type", recordType);
        properties.put("numMappers", "16");
        if (StringUtils.isNotBlank(publishConfig.getAwsRegion())) {
            properties.put("eai.export.aws.region", publishConfig.getAwsRegion());
        }
        eaiConfig.setProperties(properties);

        return eaiConfig;
    }

    private String getExportInputPath(String sourceName, String sourceVersion, PublishToDynamoConfiguration configuration) {
        return hdfsPathBuilder.constructSnapshotDir(sourceName, sourceVersion).toString();
    }

    public static String convertToFabricStoreName(String recordType) {
        return PREFIX + recordType;
    }

    // for test mock
    void setOverridingDynamoService(DynamoService dynamoService) {
        this.overridingDynamoService = dynamoService;
    }

    private DynamoService getDynamoService(PublishToDynamoConfiguration configuration) {
        if (overridingDynamoService != null) {
            // for test mock
            log.info("Using an overriding mock service, instead of the one constructed from configuration.");
            return overridingDynamoService;
        } else {
            return configurationParser.constructDynamoService(configuration);
        }
    }

    void setEaiProxy(EaiProxy eaiProxy) {
        this.eaiProxy = eaiProxy;
    }

    void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
