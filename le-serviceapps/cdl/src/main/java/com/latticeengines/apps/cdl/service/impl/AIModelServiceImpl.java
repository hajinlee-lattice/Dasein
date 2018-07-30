package com.latticeengines.apps.cdl.service.impl;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.rating.CrossSellRatingQueryBuilder;
import com.latticeengines.apps.cdl.rating.RatingQueryBuilder;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStoreName;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.AdvancedModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataStoreProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

import reactor.core.publisher.Flux;

@Component("aiModelService")
public class AIModelServiceImpl extends RatingModelServiceBase<AIModel> implements AIModelService {

    private static Logger log = LoggerFactory.getLogger(AIModelServiceImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private SegmentService segmentService;

    @Inject
    private AIModelEntityMgr aiModelEntityMgr;

    @Inject
    private PeriodService periodService;

    @Inject
    private MetadataStoreProxy metadataStoreProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private static RatingEngineType[] types = //
            new RatingEngineType[] { //
                    RatingEngineType.CROSS_SELL, //
                    RatingEngineType.CUSTOM_EVENT };

    protected AIModelServiceImpl() {
        super(Arrays.asList(types));
    }

    @Override
    public List<AIModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);
    }

    @Override
    public AIModel getRatingModelById(String id) {
        return aiModelEntityMgr.findById(id);
    }

    @Override
    public AIModel createOrUpdate(AIModel ratingModel, String ratingEngineId) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (ratingModel.getTrainingSegment() != null) {
            String segmentName = ratingModel.getTrainingSegment().getName();
            MetadataSegmentDTO segmentDTO = segmentProxy.getMetadataSegmentWithPidByName(tenant.getId(), segmentName);
            MetadataSegment segment = segmentDTO.getMetadataSegment();
            segment.setPid(segmentDTO.getPrimaryKey());
            ratingModel.setTrainingSegment(segment);
        }
        return aiModelEntityMgr.createOrUpdateAIModel(ratingModel, ratingEngineId);
    }

    @Override
    public void deleteById(String id) {
        aiModelEntityMgr.deleteById(id);
    }

    @Override
    public EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version) {
        CrossSellModelingConfig advancedConf = (CrossSellModelingConfig) aiModel.getAdvancedModelingConfig();

        if (advancedConf != null
                && Arrays.asList(ModelingStrategy.values()).contains(advancedConf.getModelingStrategy())) {
            PeriodStrategy strategy = periodService.getApsRollupPeriod(version);
            int maxPeriod = periodService.getMaxPeriodId(customerSpace, strategy, version);
            RatingQueryBuilder ratingQueryBuilder = CrossSellRatingQueryBuilder.getCrossSellRatingQueryBuilder(
                    ratingEngine, aiModel, modelingQueryType, strategy.getName(), maxPeriod);
            return ratingQueryBuilder.build();
        } else {
            throw new LedpException(LedpCode.LEDP_40009,
                    new String[] { ratingEngine.getId(), aiModel.getId(), customerSpace });
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void findRatingModelAttributeLookups(AIModel ratingModel) {
        List<MetadataSegment> segments = new ArrayList<>();
        if (ratingModel.getTrainingSegment() != null) {
            segments.add(ratingModel.getTrainingSegment());
        }
        boolean shouldHaveParentSegment = true;
        AdvancedModelingConfig advancedModelingConfig = ratingModel.getAdvancedModelingConfig();
        if (advancedModelingConfig != null && advancedModelingConfig instanceof CustomEventModelingConfig) {
            CustomEventModelingConfig customEventModelingConfig = (CustomEventModelingConfig) advancedModelingConfig;
            if (CustomEventModelingType.LPI.equals(customEventModelingConfig.getCustomEventModelingType())) {
                shouldHaveParentSegment = false;
            }
        }
        if (shouldHaveParentSegment) {
            MetadataSegment segment = aiModelEntityMgr.inflateParentSegment(ratingModel);
            if (segment != null) {
                segments.add(segment);
            }
        }
        if (CollectionUtils.isNotEmpty(segments)) {
            ratingModel.setRatingModelAttributes(new HashSet<>(segmentService.findDependingAttributes(segments)));
        }
    }

    public void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus) {
        AIModel aiModel = getRatingModelById(aiModelId);
        if (aiModel.getModelingJobStatus().isTerminated()) {
            throw new LedpException(LedpCode.LEDP_40028, new String[] { aiModelId });
        }
        aiModel.setModelingJobStatus(newStatus);
        createOrUpdate(aiModel, ratingEngineId);
        log.info(String.format("Modeling Job status updated for AIModel:%s, RatingEngine:%s to %s", aiModelId,
                ratingEngineId, newStatus.name()));
    }

    @Override
    public Map<String, List<ColumnMetadata>> getIterationMetadata(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel) {
        if (!aiModel.getModelingJobStatus().isTerminated()) {
            throw new LedpException(LedpCode.LEDP_40034,
                    new String[] { aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        if (aiModel.getModelingJobStatus() != JobStatus.COMPLETED || StringUtils.isEmpty(aiModel.getModelSummaryId())) {
            throw new LedpException(LedpCode.LEDP_40035,
                    new String[] { aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        ModelSummary modelSummary = modelSummaryProxy.getModelSummaryById(customerSpace, aiModel.getModelSummaryId());
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "ModelSummary", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }
        String tableName = modelSummary.getEventTableName();
        if (tableName == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "Event table name", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }

        Table table = metadataProxy.getTable(customerSpace, tableName);
        if (table == null) {
            throw new LedpException(LedpCode.LEDP_40036,
                    new String[] { "Event table metadata", aiModel.getId(), ratingEngine.getId(), customerSpace });
        }

        Map<String, Integer> importanceOrdering = getFeatureImportance(customerSpace, modelSummary);

        return metadataStoreProxy.getMetadata(MetadataStoreName.Table,
                CustomerSpace.shortenCustomerSpace(customerSpace), table.getName())
                .collect(HashMap<String, List<ColumnMetadata>>::new, (returnMap, cm) -> {
                    if (importanceOrdering.containsKey(cm.getAttrName())) {
                        // could move this into le-metadata as a decorator
                        cm.setImportanceOrdering(importanceOrdering.get(cm.getAttrName()));
                    }
                    if (!returnMap.containsKey(cm.getCategory().getName())) {
                        returnMap.put(cm.getCategory().getName(), new ArrayList<>());
                    }
                    returnMap.get(cm.getCategory().getName()).add(cm);
                }).block();
    }

    private Map<String, Integer> getFeatureImportance(String customerSpace, ModelSummary modelSummary) {
        try {
            String featureImportanceFilePathPattern = "{0}/{1}/models/{2}/{3}/{4}/rf_model.txt";

            String[] filePathParts = modelSummary.getLookupId().split("\\|");
            String featureImportanceFilePath = MessageFormat.format(featureImportanceFilePathPattern, //
                    modelingServiceHdfsBaseDir, // 0
                    filePathParts[0], // 1
                    filePathParts[1], // 2
                    filePathParts[2], // 3
                    modelSummary.getApplicationId().substring("application_".length())); // 4
            if (!HdfsUtils.fileExists(yarnConfiguration, featureImportanceFilePath)) {
                log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
                throw new LedpException(LedpCode.LEDP_10011, new String[] { featureImportanceFilePath });
            }
            String featureImportanceRaw = HdfsUtils.getHdfsFileContents(yarnConfiguration, featureImportanceFilePath);
            if (StringUtils.isEmpty(featureImportanceRaw)) {
                log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
                throw new LedpException(LedpCode.LEDP_40037,
                        new String[] { featureImportanceFilePath, modelSummary.getId(), customerSpace });
            }
            TreeMap<Double, String> sortedImportance = Flux.fromArray(featureImportanceRaw.split("\n"))
                    .collect(TreeMap<Double, String>::new, (sortedMap, line) -> {
                        try {
                            sortedMap.put(Double.parseDouble(line.split(",")[1]), line.split(",")[0]);
                        } catch (NumberFormatException e) {
                            // ignore since this is for the first row
                        }
                    }).block();

            AtomicInteger i = new AtomicInteger(1);
            return Flux.fromIterable(sortedImportance.entrySet())
                    .collect(HashMap<String, Integer>::new, (map, es) -> map.put(es.getValue(), i.getAndIncrement()))
                    .block();

        } catch (Exception e) {
            log.error("Unable to populate feature importance due to " + e.getLocalizedMessage());
            return new HashMap<>();
        }
    }
}
