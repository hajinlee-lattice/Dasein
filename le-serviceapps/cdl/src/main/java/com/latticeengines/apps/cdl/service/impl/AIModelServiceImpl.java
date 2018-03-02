package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import com.latticeengines.apps.cdl.rating.CrossSellRatingQueryBuilder;
import com.latticeengines.apps.cdl.rating.RatingQueryBuilder;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

@Component("aiModelService")
public class AIModelServiceImpl extends RatingModelServiceBase<AIModel> implements AIModelService {

    @Inject
    private SegmentProxy segmentProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Autowired
    private AIModelEntityMgr aiModelEntityMgr;

    protected AIModelServiceImpl() {
        super(RatingEngineType.CROSS_SELL);
    }

    @Override
    public List<AIModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);
    }

    @Override
    public AIModel geRatingModelById(String id) {
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
        if (ratingModel.getModelSummary() != null) {
            String modelSummaryId = ratingModel.getModelSummary().getId();
            if (StringUtils.isBlank(modelSummaryId)) {
                throw new IllegalArgumentException(
                        "Cannot associate ModelSummary with AIModel as ModelSummary ID is empty.");
            }
            ModelSummary selModelSummary = internalResourceProxy.getModelSummaryFromModelId(modelSummaryId,
                    CustomerSpace.parse(tenant.getId()));
            ratingModel.setModelSummary(selModelSummary);
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
            ModelingQueryType modelingQueryType) {

        if (Arrays.asList(ModelingStrategy.values()).contains(aiModel.getModelingStrategy())) {
            RatingQueryBuilder ratingQueryBuilder = CrossSellRatingQueryBuilder
                    .getCrossSellRatingQueryBuilder(ratingEngine, aiModel, modelingQueryType);
            return ratingQueryBuilder.build();
        } else {
            throw new LedpException(LedpCode.LEDP_40009,
                    new String[] { ratingEngine.getId(), aiModel.getId(), customerSpace });
        }
    }

}
