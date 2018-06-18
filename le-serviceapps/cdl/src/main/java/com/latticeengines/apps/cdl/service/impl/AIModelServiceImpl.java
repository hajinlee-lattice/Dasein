package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.rating.CrossSellRatingQueryBuilder;
import com.latticeengines.apps.cdl.rating.RatingQueryBuilder;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.PeriodService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component("aiModelService")
public class AIModelServiceImpl extends RatingModelServiceBase<AIModel> implements AIModelService {

    private static Logger log = LoggerFactory.getLogger(AIModelServiceImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

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
        aiModelEntityMgr.createOrUpdateAIModel(ratingModel, ratingEngineId);
        return ratingModel;
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
            RatingQueryBuilder ratingQueryBuilder = CrossSellRatingQueryBuilder
                    .getCrossSellRatingQueryBuilder(ratingEngine, aiModel, modelingQueryType, strategy.getName(), maxPeriod);
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
        RatingEngine parentEngine = ratingModel.getRatingEngine();
        if (parentEngine != null) {
            MetadataSegment segment = parentEngine.getSegment();
            if (segment != null) {
                segments.add(segment);
            }
        }
        ratingModel.setRatingModelAttributes(new HashSet<>(segmentService.findDependingAttributes(segments)));
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
}
