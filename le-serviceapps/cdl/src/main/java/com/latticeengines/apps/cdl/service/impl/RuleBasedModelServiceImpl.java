package com.latticeengines.apps.cdl.service.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.cdl.service.RuleBasedModelService;
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

    @Autowired
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Override
    public List<RuleBasedModel> getAllRatingModelsByRatingEngineId(String ratingEngineId) {
        return ruleBasedModelEntityMgr.findAllByRatingEngineId(ratingEngineId);
    }

    @Override
    public RuleBasedModel getRatingModelById(String id) {
        return ruleBasedModelEntityMgr.findById(id);
    }

    @Override
    public RuleBasedModel createOrUpdate(RuleBasedModel ratingModel, String ratingEngineId) {
        log.info(String.format("Creating/Updating a rule based model for Rating Engine %s", ratingEngineId));
        return ruleBasedModelEntityMgr.createOrUpdateRuleBasedModel(ratingModel, ratingEngineId);
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

        ratingModel.setRatingModelAttributes(attributes);
    }
}
