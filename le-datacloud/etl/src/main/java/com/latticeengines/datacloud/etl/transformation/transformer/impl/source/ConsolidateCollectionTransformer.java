package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.ConsolidateCollectionTransformer.TRANSFORMER_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.S3PathBuilder;
import com.latticeengines.datacloud.dataflow.transformation.source.ConsolidateCollectionAlexaFlow;
import com.latticeengines.datacloud.dataflow.transformation.source.ConsolidateCollectionBWFlow;
import com.latticeengines.datacloud.dataflow.transformation.source.ConsolidateCollectionOrbFlow;
import com.latticeengines.datacloud.dataflow.transformation.source.ConsolidateCollectionSemrushFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateCollectionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;

@Component(TRANSFORMER_NAME)
public class ConsolidateCollectionTransformer extends AbstractDataflowTransformer<ConsolidateCollectionConfig, ConsolidateCollectionParameters> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateCollectionTransformer.class);
    public static final String TRANSFORMER_NAME = "consolidateCollectionTransformer";

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Value("${datacloud.collection.ingestion.partion.period}")
    private long ingestionPeriod;//in seconds

    @Inject
    private VendorConfigMgr vendorConfigMgr;

    private Table inputTable;
    private VendorConfig vendorConfig;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        String vendor = vendorConfig.getVendor();
        switch (vendor){
            case VendorConfig.VENDOR_ALEXA:
                return ConsolidateCollectionAlexaFlow.BEAN_NAME;
            case VendorConfig.VENDOR_BUILTWITH:
                return ConsolidateCollectionBWFlow.BEAN_NAME;
            case VendorConfig.VENDOR_ORBI_V2:
                return ConsolidateCollectionOrbFlow.BEAN_NAME;
            case VendorConfig.VENDOR_SEMRUSH:
                return ConsolidateCollectionSemrushFlow.BEAN_NAME;
        }

        log.error("failure: no data-flow bean for " + vendor + " found");
        return null;
    }

    @Override
    protected boolean validateConfig(ConsolidateCollectionConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ConsolidateCollectionConfig.class;
    }

    @Override
    protected Class<ConsolidateCollectionParameters> getDataFlowParametersClass() {
        return ConsolidateCollectionParameters.class;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, //
                                         ConsolidateCollectionParameters parameters, //
                                         ConsolidateCollectionConfig configuration) {
        String vendor = configuration.getVendor();
        List<VendorConfig> vendors = vendorConfigMgr.findAll().stream().filter(config -> {
            return config.getVendor().equals(vendor);
        }).collect(Collectors.toList());
        if (vendors.size() == 0) {

            log.error("failure: unable to find VendorConfig for " + vendor);

        }
        vendorConfig = vendors.get(0);

        configParamsByVendor(vendor, parameters);

        String rawIngestion = configuration.getRawIngestion();
        inputTable = getInputTable(vendor, rawIngestion, workflowDir);
        parameters.setBaseTables(Collections.singletonList("InputTable"));

        // collect data from S3, save to a temp folder in workflowDir, use it as input of cascading dataflow
        System.out.println("Base tables: " + StringUtils.join(parameters.getBaseTables()));
    }

    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, //
                                          ConsolidateCollectionParameters paramters, //
                                          ConsolidateCollectionConfig configuration) {
        //udpate the last consolidation time stamp
        vendorConfig.setLastConsolidated(new Timestamp(System.currentTimeMillis()));
        vendorConfigMgr.update(vendorConfig);
    }

    @Override
    protected Map<String, Table> setupSourceTables(Map<Source, List<String>> baseSourceVersions) {
        Map<String, Table> sourceTables = new HashMap<>();
        sourceTables.put("InputTable", inputTable);
        return sourceTables;
    }

    /**
     * read group by keys and sort key from vendor configuration
     */
    private void configParamsByVendor(String vendor, ConsolidateCollectionParameters parameters) {
        List<String> groupBy = Arrays.asList(vendorConfig.getGroupBy().split(","));
        String sortKey = vendorConfig.getSortBy();
        parameters.setGroupBy(groupBy);
        parameters.setSortBy(sortKey);
    }

    private Table getInputTable(String vendor, String rawIngestion, String workflowDir) {
        //calc consolidation duration
        long consolidationPeriodInMS = vendorConfig.getConsolidationPeriod() * 1000;
        long ingestionPeriodInMS = ingestionPeriod * 1000;
        long curMillis = System.currentTimeMillis();
        long begMillis = curMillis - consolidationPeriodInMS;
        long begMillisAdjusted = begMillis - begMillis % ingestionPeriodInMS;
        Timestamp lastConsolidated = vendorConfig.getLastConsolidated();
        if (lastConsolidated != null) {

            log.info("last consolidation time for vendor " + vendor + ": " + lastConsolidated);
            long lastConsolidatedMills = lastConsolidated.getTime();
            lastConsolidatedMills -= lastConsolidatedMills % ingestionPeriodInMS;
            if (begMillisAdjusted < lastConsolidatedMills) {

                log.warn("notice: last consolidation  period is overlapped with current one");

            }
        }

        //date time format
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        String ingestionDir = S3PathBuilder.constructIngestionDir(rawIngestion).toS3Key();
        List<S3ObjectSummary> summaries = s3Service.listObjects(s3Bucket, ingestionDir);
        List<String> avrosToCopy = summaries.stream().map(summary -> {
            String key = summary.getKey();
            if (key.endsWith("_UTC.avro")) {
                String fileName = key.substring(key.lastIndexOf("/") + 1);
                String dateStr = fileName.substring(0, fileName.length() - "_UTC.avro".length());
                try {

                    Date date = dateFormat.parse(dateStr);
                    long millis = date.getTime();
                    if (millis >= begMillisAdjusted && millis < curMillis) {

                        return fileName;

                    }
                } catch (Exception e) {

                    log.error(key + " contains a malformed date");

                }
            }

            return null;
        }).filter(StringUtils::isNotEmpty).collect(Collectors.toList());
        log.info("Found " + CollectionUtils.size(avrosToCopy) + " avro files to copy.");

        String tableName = NamingUtils.timestamp("Input");
        String tgtDir = "/tmp/" + tableName;
        copyAvrosFromS3(tgtDir, ingestionDir, avrosToCopy);

        Table inputTable = MetadataConverter.getTable(yarnConfiguration, tgtDir);
        inputTable.setName(tableName);
        return inputTable;
    }

    private void copyAvrosFromS3(String tgtDir, String s3Dir, Collection<String> avros) {
        ExecutorService workers = ThreadPoolUtils.getFixedSizeThreadPool("ingestion-copy", 8);
        List<Runnable> runnables = new ArrayList<>();
        avros.forEach(avro -> {
            Runnable runnable = () -> {
                String key = s3Dir + "/" + avro;
                InputStream s3Stream = s3Service.readObjectAsStream(s3Bucket, key);
                try {
                    log.info("Copying " + avro + " from s3 to hdfs.");
                    HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, s3Stream, tgtDir + "/" + avro);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to copy " + avro + " from s3 to hdfs", e);
                }
            };
            runnables.add(runnable);
            if (runnables.size() >= 64) {
                ThreadPoolUtils.runRunnablesInParallel(workers, runnables, //
                        (int) TimeUnit.HOURS.toMinutes(1), 10);
            }
        });
        if (CollectionUtils.isNotEmpty(runnables)) {
            ThreadPoolUtils.runRunnablesInParallel(workers, runnables, //
                    (int) TimeUnit.HOURS.toMinutes(1), 10);
        }
        workers.shutdown();
    }

}
