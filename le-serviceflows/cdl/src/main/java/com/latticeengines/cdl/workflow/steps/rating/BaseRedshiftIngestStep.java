package com.latticeengines.cdl.workflow.steps.rating;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.callable.RedshiftIngestCallable;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.GenerateRatingStepConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

abstract class BaseRedshiftIngestStep<T extends GenerateRatingStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseRedshiftIngestStep.class);

    protected static final String MODEL_GUID = "Model_GUID";

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EventProxy eventProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Value("${cdl.redshift.query.page.size}")
    private int pageSize;

    @Value("${cdl.redshift.query.rows.per.file}")
    private int rowsPerFile;

    protected CustomerSpace customerSpace;
    protected Schema schema;
    protected DataCollection.Version version;
    protected List<RatingModelContainer> containers;
    protected String targetTableName;

    protected abstract List<RatingEngineType> getTargetEngineTypes();

    protected abstract Schema generateSchema();

    protected abstract String getTargetTableName();

    protected abstract GenericRecord parseDataForModel(String modelId, String modelGuid, Map<String, Object> data);

    @Override
    public void execute() {
        preIngestion();
        ingest();
        postIngestion();
    }

    protected void preIngestion() {
        customerSpace = configuration.getCustomerSpace();
        version = configuration.getDataCollectionVersion();
        if (version == null) {
            version = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);
            log.info("Read inactive version from workflow context: " + version);
        } else {
            log.info("Use the version specified in configuration: " + version);
        }

        List<RatingModelContainer> allContainers = getListObjectFromContext(ITERATION_RATING_MODELS,
                RatingModelContainer.class);
        containers = allContainers.stream() //
                .filter(container -> getTargetEngineTypes().contains(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no models of type " + getTargetEngineTypes() + " to be rated.");
        }

        targetTableName = getTargetTableName();
    }

    private void ingest() {
        log.info("Ingesting ratings/indicators for " + containers.size() + " rating models of type(s) "
                + getTargetEngineTypes());
        schema = generateSchema();

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
            ingestDataParallel(tableDataPath, containers);
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
    }

    protected void postIngestion() {
        String tableDataPath = PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace, "")
                .append(targetTableName).toString();
        String primaryKey = InterfaceName.__Composite_Key__.name();
        String lastModifiedKey = InterfaceName.CDLUpdatedTime.name();
        Table resultTable = MetadataConverter.getTable(yarnConfiguration, tableDataPath, primaryKey, lastModifiedKey);
        resultTable.setName(targetTableName);
        metadataProxy.createTable(customerSpace.toString(), targetTableName, resultTable);
    }

    private void ingestDataParallel(String hdfsPath, List<RatingModelContainer> containers) throws Exception {
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
                } catch (TimeoutException expected) {
                    // ignore
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Redshift ingest thread failed.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        if (CollectionUtils.isEmpty(HdfsUtils.getFilesForDir(yarnConfiguration, hdfsPath))) {
            log.warn("No ingested file in " + hdfsPath + ", write a dummy avro.");
            writeDummyOutput(hdfsPath);
        }
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath + "/_SUCCESS", "");
    }

    private void writeDummyOutput(String hdfsPath) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        GenericRecord record = builder.build();
        String targetFile = String.format("%s/dummy.avro", hdfsPath);
        RetryTemplate retry = RetryUtils.getRetryTemplate(5);
        retry.execute(ctx -> {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, targetFile);
                }
                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, Collections.singletonList(record),
                        true);
                return true;
            } catch (IOException e) {
                throw new RuntimeException("Failed to cleanup target file " + targetFile);
            }
        });
    }

    private class RedshiftIngest extends RedshiftIngestCallable<RatingModel> {

        private final RatingModel ratingModel;
        private final RatingEngineSummary engineSummary;
        private final RatingEngineType engineType;
        private String modelId;
        private String modelGuid = null;
        private MetadataSegment segment;
        private FrontEndQuery frontEndQuery = null;

        RedshiftIngest(RatingModelContainer container, String hdfsPath) {
            super(yarnConfiguration, hdfsPath);
            this.ratingModel = container.getModel();
            this.modelId = this.ratingModel.getId();
            this.engineSummary = container.getEngineSummary();
            this.engineType = container.getEngineSummary().getType();
            if (this.ratingModel instanceof AIModel) {
                this.modelGuid = ((AIModel) this.ratingModel).getModelSummaryId();
            }
            setPageSize(pageSize);
            setRowsPerFile(rowsPerFile);
        }

        @Override
        protected long getTotalCount() {
            long totalCount = totalCountInSegment();
            log.info("There are in total " + totalCount + " accounts in the segment of rating model " //
                    + ratingModel.getId());
            return totalCount;
        }

        @Override
        protected DataPage fetchPage(long ingestedCount, long pageSize) {
            if (frontEndQuery == null) {
                frontEndQuery = dataQuery();
            }
            if (pageSize > 0) {
                AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
                frontEndQuery.setSort(new FrontEndSort(Collections.singletonList(accountId), false));
                frontEndQuery.setPageFilter(new PageFilter(ingestedCount, pageSize));
            }
            DataPage dataPage;
            if (RatingEngineType.CROSS_SELL.equals(engineType)) {
                dataPage = eventProxy.getScoringTuples(customerSpace.toString(), (EventFrontEndQuery) frontEndQuery,
                        version);
            } else {
                dataPage = ratingProxy.getData(customerSpace.getTenantId(), frontEndQuery, version);
            }
            if (dataPage != null && CollectionUtils.isEmpty(dataPage.getData())) {
                log.warn("Got 0 data from api at version " + version + " for the query: " //
                        + JsonUtils.pprint(frontEndQuery));
            }
            return dataPage;
        }

        @Override
        protected GenericRecord parseData(Map<String, Object> data) {
            return parseDataForModel(modelId, modelGuid, data);
        }

        @Override
        protected Schema getAvroSchema() {
            return generateSchema();
        }

        @Override
        protected String prepareTargetFile(int fileId) {
            return prepareTargetFile(modelId, fileId);
        }

        @Override
        protected void preIngest() {
            String segmentName = engineSummary.getSegmentName();
            segment = segmentProxy.getMetadataSegmentByName(customerSpace.toString(), segmentName);
        }

        @Override
        protected RatingModel postIngest() {
            return ratingModel;
        }

        private long totalCountInSegment() {
            if (RatingEngineType.CROSS_SELL.equals(engineType)) {
                return ratingEngineProxy.getModelingQueryCountByRatingId(customerSpace.toString(),
                        engineSummary.getId(), ratingModel.getId(), ModelingQueryType.TARGET, version);
            } else if (RatingEngineType.CUSTOM_EVENT.equals(engineType)
                    || RatingEngineType.RULE_BASED.equals(engineType)) {
                FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
                int retries = 0;
                while (retries < 3) {
                    try {
                        return ratingProxy.getCountFromObjectApi(customerSpace.getTenantId(), frontEndQuery, version);
                    } catch (Exception ex) {
                        log.error("Exception in getting total count in segment for Account", ex);
                        retries++;
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            log.warn("Sleep interrupted.");
                        }
                    }
                }
                throw new RuntimeException("Fail to get total count in segment for Account");
            } else {
                throw new UnsupportedOperationException("Unknown rating engine type " + engineType);
            }
        }

        private FrontEndQuery dataQuery() {
            if (RatingEngineType.RULE_BASED.equals(engineType)) {
                return ruleBasedQuery();
            } else if (RatingEngineType.CUSTOM_EVENT.equals(engineType)) {
                return customEventQuery();
            } else if (RatingEngineType.CROSS_SELL.equals(engineType)) {
                return crossSellQuery();
            } else {
                throw new UnsupportedOperationException("Unknown rating engine type " + engineType);
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
            return frontEndQuery;
        }

        private EventFrontEndQuery crossSellQuery() {
            return ratingEngineProxy.getModelingQueryByRatingId(customerSpace.toString(),
                    engineSummary.getId(),
                    ratingModel.getId(), ModelingQueryType.TARGET);
        }

        private FrontEndQuery customEventQuery() {
            AttributeLookup accountId = new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
            FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
            frontEndQuery.setLookups(Collections.singletonList(accountId));
            return frontEndQuery;
        }

        private String prepareTargetFile(String modelId, int fileId) {
            String targetFile = String.format("%s/%s-%05d.avro", getHdfsPath(), modelId, fileId);
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, targetFile);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to cleanup target file " + targetFile);
            }
            return targetFile;
        }

    }

}
