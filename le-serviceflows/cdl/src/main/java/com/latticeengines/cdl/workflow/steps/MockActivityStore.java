package com.latticeengines.cdl.workflow.steps;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MockActivityStoreConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("mockActivitySTore")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MockActivityStore extends BaseWorkflowStep<MockActivityStoreConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(MockActivityStore.class);

    private static final String AM_ATTR_1 = "am_twv__abcdef123456__l_2_w";
    private static final String AM_ATTR_2 = "am_wvbsm__abcdef123456_123456abcdef__l_4_w";
    private static final String AM_ATTR_3 = "am_twv__abcdef123457__l_2_w";
    private static final String AM_ATTR_4 = "am_wvbsm__abcdef123457_123457abcdef__l_4_w";

    private static final Random random = new Random(System.currentTimeMillis());

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private S3Service s3Service;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private CustomerSpace customerSpace;
    private DataCollection.Version activeVersion;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        activeVersion = dataCollectionProxy.getActiveVersion(customerSpace.toString());

        String tenantId = customerSpace.getTenantId();
        List<String> accountIds = extractAccountIds();

        String tableName = tenantId + "_" + NamingUtils.timestamp(BusinessEntity.WebVisitProfile.name());
        List<Pair<String, Class<?>>> fieldTypes = Arrays.asList( //
                Pair.of(InterfaceName.AccountId.name(), String.class), //
                Pair.of(AM_ATTR_1, Long.class), //
                Pair.of(AM_ATTR_2, Long.class), //
                Pair.of(AM_ATTR_3, Long.class), //
                Pair.of(AM_ATTR_4, Long.class), //
                Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class), //
                Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class) //
        );

        Table table = constructTable(tenantId, tableName, fieldTypes, accountIds.size());
        String extractPath = table.getExtracts().get(0).getPath();
        metadataProxy.createTable(customerSpace.toString(), tableName, table);
        dataCollectionProxy.upsertTable(customerSpace.toString(), tableName, //
                TableRoleInCollection.WebVisitProfile, activeVersion);

        Schema avroSchema = AvroUtils.constructSchema(tableName, fieldTypes);
        List<GenericRecord> records = getData(avroSchema, accountIds);
        String targetFilePath = extractPath + "/mock.avro";
        try {
            AvroUtils.writeToHdfsFile(yarnConfiguration, avroSchema, targetFilePath, records);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write records to avro file " + targetFilePath, e);
        }

        StatisticsContainer statsContainer = dataCollectionProxy.getStats(customerSpace.toString(), activeVersion);
        mergeStats(statsContainer, records);
        statsContainer.setName(NamingUtils.timestamp("Stats"));
        dataCollectionProxy.upsertStats(customerSpace.toString(), statsContainer);

        exportTableRoleToRedshift(tableName, extractPath);
        exportToDynamo(tableName, extractPath);
        dataCollectionProxy.clearCache(customerSpace.toString());
        servingStoreProxy.getDecoratedMetadata(customerSpace.toString(), BusinessEntity.Account, null);
    }

    private List<String> extractAccountIds() {
        String accountTableName = dataCollectionProxy //
                .getTableName(customerSpace.toString(), TableRoleInCollection.ConsolidatedAccount, activeVersion);
        if (StringUtils.isBlank(accountTableName)) {
            throw new IllegalStateException("There is no account batch store");
        }
        log.info("Found account batch store named {}", accountTableName);
        S3DataUnit s3Unit = //
                (S3DataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(), accountTableName, DataUnit.StorageType.S3);
        if (s3Unit == null) {
            throw new IllegalStateException("There is no S3 data unit for account batch store");
        }
        String s3Url = s3Unit.getFullPath(useEmr ? "s3a" : "s3n");
        log.info("Found S3 location for account batch store: {}", s3Url);

        Pattern pattern = Pattern.compile("s3a://(?<bucket>[^/]+)/(?<prefix>.*)");
        Matcher matcher = pattern.matcher(s3Url);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse s3 url " + s3Url);
        }
        String bucket = matcher.group("bucket");
        String prefix = matcher.group("prefix");

        Iterator<InputStream> isItr= s3Service.getObjectStreamIterator(bucket, prefix, new S3KeyFilter() {
        });
        try (AvroUtils.AvroStreamsIterator iter = AvroUtils.iterateAvroStreams(isItr)) {
            List<String> accountIds = new ArrayList<>();
            Iterable<GenericRecord> iterable = () -> iter;
            for (GenericRecord record: iterable) {
                String accountId = record.get(InterfaceName.AccountId.name()).toString();
                accountIds.add(accountId);
                if (accountIds.size() >= 1000) {
                    break;
                }
            }
            log.info("Extracted {} account ids from batch store in s3.", accountIds.size());
            return accountIds;
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract account ids from avro files in s3, " + s3Url, e);
        }
    }

    private List<GenericRecord> getData(Schema avroSchema, List<String> accountIds) {
        List<GenericRecord> records = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        for (String accountId: accountIds) {
            GenericRecordBuilder builder = new GenericRecordBuilder(avroSchema);
            builder.set(InterfaceName.AccountId.name(), accountId);

            builder.set(AM_ATTR_1, generateVisits());
            builder.set(AM_ATTR_2, generateVisits());
            builder.set(AM_ATTR_3, generateVisits());
            builder.set(AM_ATTR_4, generateVisits());

            builder.set(InterfaceName.CDLCreatedTime.name(), currentTime);
            builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
            records.add(builder.build());
        }
        return records;
    }

    private void mergeStats(StatisticsContainer stats, List<GenericRecord> records) {
        Map<String, StatsCube> cubes = stats.getStatsCubes();
        StatsCube cube = getStatsCube(records);
        cubes.put(BusinessEntity.WebVisitProfile.name(), cube);
        stats.setStatsCubes(cubes);
    }

    private StatsCube getStatsCube(List<GenericRecord> records) {
        Map<String, AttributeStats> statsMap = new HashMap<>();
        statsMap.put(AM_ATTR_1, calcAttrStats(AM_ATTR_1, records, records.size()));
        statsMap.put(AM_ATTR_2, calcAttrStats(AM_ATTR_2, records, records.size()));
        statsMap.put(AM_ATTR_3, calcAttrStats(AM_ATTR_3, records, records.size()));
        statsMap.put(AM_ATTR_4, calcAttrStats(AM_ATTR_4, records, records.size()));

        StatsCube cube = new StatsCube();
        cube.setCount((long) records.size());
        cube.setStatistics(statsMap);
        log.info("Generated stats cube: " + JsonUtils.serialize(cube));
        return cube;
    }

    private AttributeStats calcAttrStats(String fieldName, List<GenericRecord> records, long cnt) {
        AttributeStats attrStats = new AttributeStats();
        attrStats.setNonNullCount(cnt);
        attrStats.setBuckets(collectBuckets(records, fieldName));
        return attrStats;
    }

    private Buckets collectBuckets(List<GenericRecord> records, String fieldName) {
        AtomicLong bkt1Cnt = new AtomicLong(0); // 1-9
        AtomicLong bkt2Cnt = new AtomicLong(0); // 10-99
        AtomicLong bkt3Cnt = new AtomicLong(0); // 100-999
        AtomicLong bkt4Cnt = new AtomicLong(0); // >=1000

        for (GenericRecord record: records) {
            long val = (long) record.get(fieldName);
            if (val < 10) {
                bkt1Cnt.incrementAndGet();
            } else if (val < 100) {
                bkt2Cnt.incrementAndGet();
            } else if (val < 1000) {
                bkt3Cnt.incrementAndGet();
            } else {
                bkt4Cnt.incrementAndGet();
            }
        }

        Bucket bkt1 = Bucket.rangeBkt(null, 10);
        bkt1.setId(1L);
        bkt1.setLabel("< 10");
        bkt1.setCount(bkt1Cnt.get());
        Bucket bkt2 = Bucket.rangeBkt(10, 100);
        bkt2.setId(2L);
        bkt2.setLabel("10 - 100");
        bkt2.setCount(bkt2Cnt.get());
        Bucket bkt3 = Bucket.rangeBkt(100, 1000);
        bkt3.setId(3L);
        bkt3.setLabel("100 - 1000");
        bkt3.setCount(bkt3Cnt.get());
        Bucket bkt4 = Bucket.rangeBkt(1000, null);
        bkt4.setId(4L);
        bkt4.setLabel(">= 1000");
        bkt4.setCount(bkt4Cnt.get());

        Buckets bkts = new Buckets();
        bkts.setType(BucketType.Numerical);
        bkts.setBucketList(Arrays.asList(bkt1, bkt2, bkt3, bkt4));
        return bkts;
    }

    private Table constructTable(String tenantId, String tableName, List<Pair<String, Class<?>>> fields, long count) {
        Extract extract = new Extract();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        String hdfsPath = PathBuilder.buildDataTablePath("QA", customerSpace).append(tableName).toString();
        extract.setExtractionTimestamp(System.currentTimeMillis());
        extract.setPath(hdfsPath);
        extract.setProcessedRecords(count);
        extract.setName(NamingUtils.timestamp("Extract"));
        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);
        table.setAttributes(getAttributes(fields));
        table.setTableType(TableType.DATATABLE);
        table.setExtracts(Collections.singletonList(extract));
        return table;
    }

    private List<Attribute> getAttributes(List<Pair<String, Class<?>>> fields) {
        List<Attribute> attributes = new ArrayList<>();
        for (Pair<String, Class<?>> field: fields) {
            String attrName = field.getLeft();

            Attribute attribute = new Attribute(attrName);
            attribute.setPhysicalDataType(field.getRight().getSimpleName());
            attribute.setCategory(Category.WEB_VISIT_PROFILE);
            switch (attrName) {
                case "AccountId":
                    attribute.setDisplayName("Account Id");
                    break;
                case "CDLCreatedTime":
                    attribute.setDisplayName("Created Time");
                    break;
                case "CDLUpdatedTime":
                    attribute.setDisplayName("Updated Time");
                    break;
                case AM_ATTR_1:
                    attribute.setDisplayName("Visited in last 2 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 1 in last 2 weeks");
                    attribute.setSubcategory("Path 1");
                    break;
                case AM_ATTR_2:
                    attribute.setDisplayName("Visited in last 4 weeks from Source 1");
                    attribute.setDescription("Number of visits on a url matching Path 1 from Source 1 in last 4 weeks");
                    attribute.setSubcategory("Path 1");
                    break;
                case AM_ATTR_3:
                    attribute.setDisplayName("Visited in last 2 weeks");
                    attribute.setDescription("Number of visits on a url matching Path 2 in last 2 weeks");
                    attribute.setSubcategory("Path 2");
                    break;
                case AM_ATTR_4:
                    attribute.setDisplayName("Visited in last 4 weeks from Source 2");
                    attribute.setDescription("Number of visits on a url matching Path 2 from Source 2 in last 4 weeks");
                    attribute.setSubcategory("Path 2");
                    break;
                default:
            }

            attributes.add(attribute);
        }
        return attributes;
    }

    /**
     * 1-9: 50%
     * 10-99: 25%
     * 100-999: 15%
     * >=1000: 10%
     */
    private long generateVisits() {
        int rnd = random.nextInt(100);
        if (rnd < 50) {
            return random.nextInt(10);
        } else if (rnd < 75) {
            return 10 + random.nextInt(90);
        } else if (rnd < 90) {
            return 100 + random.nextInt(900);
        } else {
            return 1000 + random.nextInt(10000);
        }
    }

    private void exportToDynamo(String tableName, String inputPath) {
        DynamoExportConfig config = new DynamoExportConfig();
        config.setTableName(tableName);
        config.setInputPath(inputPath);
        config.setPartitionKey(InterfaceName.AccountId.name());
        addToListInContext(TABLES_GOING_TO_DYNAMO, config, DynamoExportConfig.class);
    }

    protected void exportTableRoleToRedshift(String tableName, String inputPath) {
        String partition = null;
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus != null && dcStatus.getDetail() != null) {
            partition = dcStatus.getRedshiftPartition();
        }

        RedshiftExportConfig config = new RedshiftExportConfig();
        config.setTableName(tableName);
        config.setDistKey(InterfaceName.AccountId.name());
        config.setDistStyle(RedshiftTableConfiguration.DistStyle.All);
        config.setInputPath(inputPath + "/*.avro");
        config.setUpdateMode(false);
        config.setClusterPartition(partition);
        addToListInContext(TABLES_GOING_TO_REDSHIFT, config, RedshiftExportConfig.class);
    }

}
