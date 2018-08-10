package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.service.RuleBasedModelService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;

@Component("ruleBasedModelService")
public class RuleBasedModelServiceImpl extends RatingModelServiceBase<RuleBasedModel> implements RuleBasedModelService {

    protected RuleBasedModelServiceImpl() {
        super(RatingEngineType.RULE_BASED);
    }

    private static final Logger log = LoggerFactory.getLogger(RuleBasedModelServiceImpl.class);

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Inject
    private SegmentService segmentService;

    @Override
    public List<RuleBasedModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    public RuleBasedModel createNewIteration(RuleBasedModel aiModel, RatingEngine ratingEngine) {
        throw new LedpException(LedpCode.LEDP_32001,
                new String[] { "RuleBased Models do not support multiple iterations" });
    }

    @Override
    public RuleBasedModel getRatingModelById(String id) {
        return ruleBasedModelEntityMgr.findById(id);
    }

    @Override
    public RuleBasedModel createOrUpdate(RuleBasedModel ratingModel, String ratingEngineId) {
        log.info(String.format("Creating/Updating a rule based model for Rating Engine %s", ratingEngineId));
        findRatingModelAttributeLookups(ratingModel);
        if (ratingModel.getId() == null) {
            ratingModel.setId(RuleBasedModel.generateIdStr());
            log.info(String.format("Creating a rule based model with id %s for ratingEngine %s", ratingModel.getId(),
                    ratingEngineId));
            return ruleBasedModelEntityMgr.createRuleBasedModel(ratingModel, ratingEngineId);
        } else {
            RuleBasedModel retrievedRuleBasedModel = ruleBasedModelEntityMgr.findById(ratingModel.getId());
            if (retrievedRuleBasedModel == null) {
                log.warn(String.format("RuleBasedModel with id %s is not found. Creating a new one",
                        ratingModel.getId()));
                return ruleBasedModelEntityMgr.createRuleBasedModel(ratingModel, ratingEngineId);
            } else {
                findRatingModelAttributeLookups(retrievedRuleBasedModel);
                return ruleBasedModelEntityMgr.updateRuleBasedModel(ratingModel, retrievedRuleBasedModel,
                        ratingEngineId);
            }
        }
    }

    @Override
    public void deleteById(String Id) {
        ruleBasedModelEntityMgr.deleteById(Id);
    }

    @Override
    public void findRatingModelAttributeLookups(RuleBasedModel ratingModel) {
        TreeMap<String, Map<String, Restriction>> rulesMap = ratingModel.getRatingRule().getBucketToRuleMap();
        Iterator it = rulesMap.keySet().iterator();
        Set<AttributeLookup> attributes = new HashSet<>();
        while (it.hasNext()) {
            Map<String, Restriction> rules = rulesMap.get(it.next());
            for (Map.Entry<String, Restriction> entry : rules.entrySet()) {
                attributes.addAll(RestrictionUtils.getRestrictionDependingAttributes(entry.getValue()));
            }
        }
        MetadataSegment segment = ruleBasedModelEntityMgr.inflateParentSegment(ratingModel);
        if (segment != null) {
            attributes.addAll(segmentService.findDependingAttributes(Collections.singletonList(segment)));
        }
        ratingModel.setRatingModelAttributes(attributes);
    }
}
