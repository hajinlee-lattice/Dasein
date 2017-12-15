package com.latticeengines.cdl.workflow.steps.rating;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("ingestRatingFromRedshift")
public class IngestRatingFromRedshift extends BaseWorkflowStep<GenerateRatingStepConfiguration> {

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private EntityProxy entityProxy;

    private CustomerSpace customerSpace;
    private Schema schema;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        String targetTableName = getObjectFromContext(RAW_RATING_TABLE_NAME, String.class);
        if (StringUtils.isBlank(targetTableName)) {
            throw new IllegalStateException("Must specify RAW_RATING_TABLE_NAME in workflow context");
        }

        List<RatingModelContainer> containers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no models to be rated.");
        }

        if (containers.size() != 1) {
            throw new UnsupportedOperationException("Cannot handle single rule based rating engine for now.");
        }

        log.info("Ingesting ratings/indicators for " + containers.size() + " rating models.");
        schema = generateSchema(containers);
        log.info("Generated avro schema: " + schema.toString(true));

        String tableDataPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "")
                .append(targetTableName).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, tableDataPath)) {
                throw new IllegalArgumentException("Target table path " + tableDataPath + " is already occupied.");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to check target table path.", e);
        }
        try {
            ingestData(tableDataPath, containers);
        } catch (Exception e) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, tableDataPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, tableDataPath);
                }
            } catch (IOException e1) {
                throw new RuntimeException("Failed to cleanup target table path.", e1);
            }
            throw new RuntimeException(e);
        }

        String primaryKey = InterfaceName.AccountId.name();
        String lastModifiedKey = InterfaceName.CDLUpdatedTime.name();
        Table resultTable = MetadataConverter.getTable(yarnConfiguration, tableDataPath, primaryKey, lastModifiedKey);
        resultTable.setName(targetTableName);
        metadataProxy.createTable(customerSpace.toString(), targetTableName, resultTable);
    }

    private Schema generateSchema(List<RatingModelContainer> containers) {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        containers.forEach(container -> {
            RatingEngineType engineType;
            try {
                engineType = container.getEngineSummary().getType();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Cannot determine the engine type of rating model " + JsonUtils.pprint(container));
            }
            if (RatingEngineType.RULE_BASED.equals(engineType)) {
                columns.add(Pair.of(container.getModel().getId(), String.class));
            } else if (RatingEngineType.AI_BASED.equals(engineType)) {
                columns.add(Pair.of(container.getModel().getId(), Boolean.class));
            } else {
                throw new IllegalArgumentException("Unknown engine type: " + engineType);
            }
        });
        columns.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));
        columns.add(Pair.of(InterfaceName.CDLUpdatedTime.name(), Long.class));
        return AvroUtils.constructSchema("Rating", columns);
    }

    private void ingestData(String hdfsPath, List<RatingModelContainer> containers) throws Exception {
        int poolSize = Math.min(3, containers.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("redshift-ingest", poolSize);
        List<Future<RatingModel>> futures = new ArrayList<>();
        containers.forEach(container -> futures.add(executorService.submit(new RedshiftIngest(container, hdfsPath))));
        while (!futures.isEmpty()) {
            List<Future<RatingModel>> completed = new ArrayList<>();
            futures.forEach(future -> {
                try {
                    RatingModel model = future.get(1, TimeUnit.SECONDS);
                    completed.add(future);
                    log.info("Finished ingesting rating model " + model.getId());
                } catch (TimeoutException e) {
                    // ignore
                } catch (InterruptedException|ExecutionException e) {
                    throw new RuntimeException("Redshift ingest thread failed.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath + "/_SUCCESS", "");
    }

    private class RedshiftIngest implements Callable<RatingModel> {

        private final int PAGE_SIZE = 250;
        private final RatingModel ratingModel;
        private final RatingEngineSummary engineSummary;
        private final RatingEngineType engineType;
        private final String hdfsPath;

        private long totalCount;
        private long ingestedCount = 0L;

        private MetadataSegment segment;
        private String targetFile;

        RedshiftIngest(RatingModelContainer container, String hdfsPath) {
            this.ratingModel = container.getModel();
            this.engineSummary = container.getEngineSummary();
            this.engineType = container.getEngineSummary().getType();
            this.hdfsPath = hdfsPath;
        }

        private long totalCountInSegment() {
            FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
            return entityProxy.getCountFromObjectApi(customerSpace.getTenantId(), frontEndQuery);
        }

        private void ingestPageByPage() {
            boolean firstPage = true;
            long offset = 0;
            FrontEndQuery frontEndQuery = dataQuery();
            List<Map<String, Object>> data;
            do {
                frontEndQuery.setPageFilter(new PageFilter(offset, PAGE_SIZE));
                DataPage dataPage = ratingProxy.getDataFromObjectApi(customerSpace.getTenantId(), frontEndQuery);
                data = dataPage.getData();
                if (CollectionUtils.isNotEmpty(data)) {
                    List<GenericRecord> records = new ArrayList<>();
                    long currentTime = System.currentTimeMillis();
                    data.forEach(map -> {
                        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                        map.forEach(builder::set);
                        if (RatingEngineType.AI_BASED.equals(engineType)) {
                            builder.set(ratingModel.getId(), true);
                        }
                        builder.set(InterfaceName.CDLCreatedTime.name(), currentTime);
                        builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
                        records.add(builder.build());
                    });
                    if (firstPage) {
                        try {
                            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, records, true);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to write to avro file", e);
                        }
                        firstPage = false;
                    } else {
                        try {
                            AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, records, true);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to write to avro file", e);
                        }
                    }
                    ingestedCount += records.size();
                    log.info(String.format("Ingested %d / %d records for rating model %s", ingestedCount, totalCount,
                            ratingModel.getId()));
                }
                offset += PAGE_SIZE;
            } while (CollectionUtils.isNotEmpty(data));
        }

        private FrontEndQuery dataQuery() {
            if (RatingEngineType.RULE_BASED.equals(engineType)) {
                return ruleBasedQuery();
            } else {
                return aiBasedQuery();
            }
        }

        private FrontEndQuery ruleBasedQuery() {
            AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
            FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
            frontEndQuery.setRatingModels(Collections.singletonList(ratingModel));
            frontEndQuery.setLookups(Arrays.asList( //
                    accountId, //
                    new AttributeLookup(BusinessEntity.Rating, ratingModel.getId()) //
            ));
            frontEndQuery.setSort(new FrontEndSort(Collections.singletonList(accountId), false));
            return frontEndQuery;
        }

        private FrontEndQuery aiBasedQuery() {
            AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
            FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
            frontEndQuery.setLookups(Collections.singletonList(accountId));
            frontEndQuery.setSort(new FrontEndSort(Collections.singletonList(accountId), false));
            return frontEndQuery;
        }

        private String prepareTargetFile(String hdfsPath, String modelId) {
            String targetFile = hdfsPath + "/" + modelId + ".avro";
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, targetFile);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to cleanup target file " + targetFile);
            }
            return targetFile;
        }

        @Override
        public RatingModel call() {
            String segmentName = engineSummary.getSegmentName();
            segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), segmentName);
            totalCount = totalCountInSegment();
            log.info("There are in total " + totalCount + " accounts in the segment of rating model " //
                    + ratingModel.getId());
            targetFile = prepareTargetFile(hdfsPath, ratingModel.getId());
            ingestPageByPage();
            return ratingModel;
        }

    }

}
