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
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.cdl.workflow.steps.CloneTableService;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
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
import com.latticeengines.proxy.exposed.objectapi.RatingProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("ingestRuleBasedRating")
public class IngestRuleBasedRating extends BaseWorkflowStep<GenerateRatingStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(IngestRuleBasedRating.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private RatingProxy ratingProxy;

    @Inject
    private CloneTableService cloneTableService;

    private CustomerSpace customerSpace;
    private Schema schema;
    private DataCollection.Version inactive;

    @Override
    public void execute() {
        customerSpace = configuration.getCustomerSpace();
        inactive = getObjectFromContext(CDL_INACTIVE_VERSION, DataCollection.Version.class);

        cloneServingStores();

        String targetTableName = getObjectFromContext(RAW_RATING_TABLE_NAME, String.class);
        if (StringUtils.isBlank(targetTableName)) {
            throw new IllegalStateException("Must specify RAW_RATING_TABLE_NAME in workflow context");
        }

        List<RatingModelContainer> allContainers = getListObjectFromContext(RATING_MODELS, RatingModelContainer.class);
        List<RatingModelContainer> containers = allContainers.stream() //
                .filter(container -> RatingEngineType.RULE_BASED.equals(container.getEngineSummary().getType())) //
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(containers)) {
            throw new IllegalStateException("There is no rule based models to be rated.");
        }

        log.info("Ingesting ratings/indicators for " + containers.size() + " rule based rating models.");
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
        PrimaryKey pk = new PrimaryKey();
        pk.setName("AccountModelIds");
        pk.setDisplayName("AccountModelIds");
        pk.addAttribute(InterfaceName.AccountId.name());
        pk.addAttribute(InterfaceName.ModelId.name());
        resultTable.setPrimaryKey(pk);
        metadataProxy.createTable(customerSpace.toString(), targetTableName, resultTable);
    }

    private Schema generateSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        columns.add(Pair.of(InterfaceName.ModelId.name(), String.class));
        columns.add(Pair.of(InterfaceName.Rating.name(), String.class));
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
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Redshift ingest thread failed.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        HdfsUtils.writeToFile(yarnConfiguration, hdfsPath + "/_SUCCESS", "");
    }

    private void cloneServingStores() {
        DataCollection.Version active = getObjectFromContext(CDL_ACTIVE_VERSION, DataCollection.Version.class);
        cloneTableService.setActiveVersion(active);
        cloneTableService.setCustomerSpace(customerSpace);
        Arrays.stream(BusinessEntity.values()).forEach(entity -> {
            TableRoleInCollection servingStore = entity.getServingStore();
            if (servingStore != null) {
                cloneTableService.cloneToInactiveTable(servingStore);
            }
        });
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

        private MetadataSegment segment;

        RedshiftIngest(RatingModelContainer container, String hdfsPath) {
            this.ratingModel = container.getModel();
            this.engineSummary = container.getEngineSummary();
            this.engineType = container.getEngineSummary().getType();
            this.hdfsPath = hdfsPath;
        }

        private long totalCountInSegment() {
            FrontEndQuery frontEndQuery = segment.toFrontEndQuery(BusinessEntity.Account);
            return ratingProxy.getCountFromObjectApi(customerSpace.getTenantId(), frontEndQuery, inactive);
        }

        private void ingestPageByPage() {
            boolean firstPage = true;
            int recordsInCurrentFile = 0;
            int fileId = 0;
            FrontEndQuery frontEndQuery = dataQuery();
            List<Map<String, Object>> data;
            do {
                frontEndQuery.setPageFilter(new PageFilter(ingestedCount, PAGE_SIZE));
                DataPage dataPage = ratingProxy.getDataFromObjectApi(customerSpace.getTenantId(), frontEndQuery,
                        inactive);
                data = dataPage.getData();
                if (CollectionUtils.isNotEmpty(data)) {
                    List<GenericRecord> records = dataPageToRecords(dataPage);
                    recordsInCurrentFile += records.size();
                    String targetFile = prepareTargetFile(hdfsPath, ratingModel.getId(), fileId);
                    if (firstPage) {
                        log.info("Start dumping records to " + targetFile);
                        try {
                            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetFile, records, true);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to write to avro file " + targetFile, e);
                        }
                        firstPage = false;
                    } else {
                        try {
                            AvroUtils.appendToHdfsFile(yarnConfiguration, targetFile, records, true);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to append to avro file " + targetFile, e);
                        }
                        if (recordsInCurrentFile >= ROWS_PER_FILE) {
                            fileId++;
                            firstPage = true;
                            recordsInCurrentFile = 0;
                        }
                    }
                    ingestedCount += records.size();
                    log.info(String.format("Ingested %d / %d records for rating model %s", ingestedCount, totalCount,
                            ratingModel.getId()));
                }
            } while (CollectionUtils.isNotEmpty(data));
        }

        private List<GenericRecord> dataPageToRecords(DataPage dataPage) {
            List<Map<String, Object>> data = dataPage.getData();
            List<GenericRecord> records = new ArrayList<>();
            long currentTime = System.currentTimeMillis();
            data.forEach(map -> {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                String accountIdAttr = InterfaceName.AccountId.name();
                builder.set(accountIdAttr, map.get(accountIdAttr));
                builder.set(InterfaceName.ModelId.name(), ratingModel.getId());
                builder.set(InterfaceName.Rating.name(), map.get(ratingModel.getId()));
                builder.set(InterfaceName.CDLCreatedTime.name(), currentTime);
                builder.set(InterfaceName.CDLUpdatedTime.name(), currentTime);
                records.add(builder.build());
            });
            return records;
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
