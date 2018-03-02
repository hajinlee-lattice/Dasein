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
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
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
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.objectapi.EventProxy;
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

import reactor.core.publisher.Mono;

abstract class BaseRedshiftIngestStep<T extends GenerateRatingStepConfiguration> extends BaseWorkflowStep<T> {

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

    protected CustomerSpace customerSpace;
    protected Schema schema;
    protected DataCollection.Version inactive;
    protected List<RatingModelContainer> containers;
    protected String targetTableName;

    protected abstract RatingEngineType getTargetEngineType();
    protected abstract Schema generateSchema();
    protected abstract List<GenericRecord> dataPageToRecords(String modelId, String modelGuid, List<Map<String, Object>> data);

    @Override
    public void execute() {
        preIngestion();
        ingest();
        postIngestion();
    }

    private void preIngestion() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        containers = allContainers.stream() //
                .filter(container -> getTargetEngineType().equals(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no models of type " + getTargetEngineType() + " to be rated.");
        }

        targetTableName = NamingUtils.timestamp(getTargetEngineType().name());
    }

    private void ingest() {
        log.info("Ingesting ratings/indicators for " + containers.size() + " rating models of type "
                + getTargetEngineType());
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
                } catch (TimeoutException e) {
                    // ignore
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Redshift ingest thread failed.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath + "/_SUCCESS", "");
    }

    private class RedshiftIngest implements Callable<RatingModel> {

        private final int PAGE_SIZE = 10000;
        private final int ROWS_PER_FILE = 1000_000;
        private final RatingModel ratingModel;
        private final RatingEngineSummary engineSummary;
        private final RatingEngineType engineType;
        private final String hdfsPath;

        private long totalCount;
        private long ingestedCount = 0L;

        private String modelId;
        private String modelGuid = null;

        private MetadataSegment segment;

        RedshiftIngest(RatingModelContainer container, String hdfsPath) {
            this.ratingModel = container.getModel();
            this.modelId = this.ratingModel.getId();
            this.engineSummary = container.getEngineSummary();
            this.engineType = container.getEngineSummary().getType();
            if (RatingEngineType.CROSS_SELL.equals(this.engineType)) {
                this.modelGuid = ((AIModel) this.ratingModel).getModelGuid();
            }
            this.hdfsPath = hdfsPath;
        }

        private long totalCountInSegment() {
            if (RatingEngineType.RULE_BASED.equals(engineType)) {
                FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
                return ratingProxy.getCountFromObjectApi(customerSpace.getTenantId(), frontEndQuery, inactive);
            } else {
                return ratingEngineProxy.getModelingQueryCountByRatingId(customerSpace.toString(), engineSummary.getId(),
                        ratingModel.getId(), ModelingQueryType.TARGET);
            }
        }

        private void ingestPageByPage() {
            int recordsInCurrentFile = 0;
            int fileId = 0;
            String targetFile = prepareTargetFile(hdfsPath, ratingModel.getId(), fileId);
            FrontEndQuery frontEndQuery = dataQuery();
            List<Map<String, Object>> data = new ArrayList<>();
            do {
                frontEndQuery.setPageFilter(new PageFilter(ingestedCount, PAGE_SIZE));
                Mono<DataPage> dataPageMono;
                if (RatingEngineType.RULE_BASED.equals(engineType)) {
                    dataPageMono = ratingProxy.getDataNonBlocking(customerSpace.getTenantId(), frontEndQuery, inactive);
                } else {
                    dataPageMono = eventProxy.getScoringTuplesNonBlocking(customerSpace.toString(), (EventFrontEndQuery) frontEndQuery,
                            inactive);
                }
                try (PerformanceTimer timer = new PerformanceTimer()) {
                    DataPage dataPage = dataPageMono.block();
                    if (dataPage != null) {
                        data = dataPage.getData();
                    }
                    String msg = "Fetched a page of " + data.size() + " tuples.";
                    timer.setTimerMessage(msg);
                }
                if (CollectionUtils.isNotEmpty(data)) {
                    List<GenericRecord> records = dataPageToRecords(modelId, modelGuid, data);
                    recordsInCurrentFile += records.size();
                    synchronized (this) {
                        try {
                            if (!HdfsUtils.fileExists(yarnConfiguration, targetFile)) {
                                log.info("Start dumping " + records.size() + " records to " + targetFile);
                                AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, records, true);
                            } else {
                                log.info("Appending " + records.size() + " records to " + targetFile);
                                AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, records, true);
                                if (recordsInCurrentFile >= ROWS_PER_FILE) {
                                    fileId++;
                                    targetFile = prepareTargetFile(hdfsPath, ratingModel.getId(), fileId);
                                    recordsInCurrentFile = 0;
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to write to avro file " + targetFile, e);
                        }
                    }
                    ingestedCount += records.size();
                    log.info(String.format("Ingested %d / %d records for rating model %s", ingestedCount, totalCount,
                            ratingModel.getId()));
                }
            } while (ingestedCount < totalCount && CollectionUtils.isNotEmpty(data));
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

        private EventFrontEndQuery aiBasedQuery() {
            return ratingEngineProxy.getModelingQueryByRatingId(customerSpace.toString(),
                    engineSummary.getId(), ratingModel.getId(), ModelingQueryType.TARGET);
        }

        private String prepareTargetFile(String hdfsPath, String modelId, int fileId) {
            String targetFile = String.format("%s/%s-%05d.avro", hdfsPath, modelId, fileId);
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
            ingestPageByPage();
            return ratingModel;
        }

    }

}
