package com.latticeengines.apps.cdl.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.RuleBasedModelEntityMgr;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.util.RestrictionUtils;

@Component
public class RuleBasedModelDependencyUtil {

    @Inject
    private RuleBasedModelEntityMgr ruleBasedModelEntityMgr;

    @Inject
    private SegmentDependencyUtil segmentDependencyUtil;

    @NoCustomerSpace
    public void findRatingModelAttributeLookups(RuleBasedModel ratingModel) {
        Set<AttributeLookup> attributes = new HashSet<>();
        if (ratingModel != null && ratingModel.getRatingRule() != null) {
            TreeMap<String, Map<String, Restriction>> rulesMap = ratingModel.getRatingRule().getBucketToRuleMap();
            Iterator<?> it = rulesMap.keySet().iterator();
            while (it.hasNext()) {
                Map<String, Restriction> rules = rulesMap.get(it.next());
                for (Map.Entry<String, Restriction> entry : rules.entrySet()) {
                    attributes.addAll(RestrictionUtils.getRestrictionDependingAttributes(entry.getValue()));
                }
            }
        }
        if (ratingModel != null) {
            MetadataSegment segment = ruleBasedModelEntityMgr.inflateParentSegment(ratingModel);
            if (segment != null) {
                attributes.addAll(segmentDependencyUtil.findDependingAttributes(Collections.singletonList(segment)));
            }
        }
        ratingModel.setRatingModelAttributes(attributes);
    }

}
