package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.ConsolidateCollectionTransformer.TRANSFORMER_NAME;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.latticeengines.datacloud.dataflow.transformation.ConsolidateCollectionFlow;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateCollectionConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;

@Component(TRANSFORMER_NAME)
public class ConsolidateCollectionTransformer extends AbstractDataflowTransformer<ConsolidateCollectionConfig, ConsolidateCollectionParameters> {

    private static final Logger log = LoggerFactory.getLogger(ConsolidateCollectionTransformer.class);
    public static final String TRANSFORMER_NAME = "consolidateCollectionTransformer";

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    private Table inputTable;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return ConsolidateCollectionFlow.BEAN_NAME;
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
        // if nothing to do in post processing, can remove this override
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
        //FIXME: configure based on vendor config table
        List<String> groupBy = Arrays.asList("Domain", "Technology_Name");
        String sortKey = "Last_Modification_Date";
        parameters.setGroupBy(groupBy);
        parameters.setSortBy(sortKey);
    }

    private Table getInputTable(String vendor, String rawIngestion, String workflowDir) {
        String ingestionDir = S3PathBuilder.constructIngestionDir(rawIngestion).toS3Key();
        List<S3ObjectSummary> summaries = s3Service.listObjects(s3Bucket, ingestionDir);
        List<String> avrosToCopy = summaries.stream().map(summary -> {
            String key = summary.getKey();
            if (key.endsWith(".avro")) {
                //FIXME: filter avros within the vendor's consolidation period.
                return key.substring(key.lastIndexOf("/") + 1);
            } else {
                return "";
            }
        }).filter(StringUtils::isNotBlank).collect(Collectors.toList());
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
