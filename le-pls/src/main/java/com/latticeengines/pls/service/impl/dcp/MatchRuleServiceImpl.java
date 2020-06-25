package com.latticeengines.pls.service.impl.dcp;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.domain.exposed.dcp.match.MatchRuleRecord;
import com.latticeengines.pls.service.dcp.MatchRuleService;

@Service("matchRuleService")
public class MatchRuleServiceImpl implements MatchRuleService {

    @Override
    public MatchRule updateMatchRule(MatchRule matchRule) {
        return getMockMatchRule(matchRule);
    }

    @Override
    public MatchRule createMatchRule(MatchRule matchRule) {
        return getMockMatchRule(matchRule);
    }

    @Override
    public List<MatchRule> getMatchRuleList(String sourceId, Boolean includeArchived, Boolean includeInactive) {
        return getMockMatchRuleList(sourceId);
    }

    @Override
    public void archiveMatchRule(String matchRuleId) {

    }

    private MatchRule getMockMatchRule(MatchRule matchRule) {
        MatchRule mockRule = new MatchRule();
        mockRule.setSourceId(matchRule.getSourceId());
        mockRule.setDisplayName(matchRule.getDisplayName());
        mockRule.setRuleType(matchRule.getRuleType());
        mockRule.setMatchKey(matchRule.getMatchKey());
        mockRule.setAllowedValues(matchRule.getAllowedValues());
        mockRule.setExclusionCriterionList(matchRule.getExclusionCriterionList());
        mockRule.setAcceptCriterion(matchRule.getAcceptCriterion());
        mockRule.setReviewCriterion(matchRule.getReviewCriterion());
        if (StringUtils.isNotEmpty(matchRule.getMatchRuleId())) {
            mockRule.setMatchRuleId(matchRule.getMatchRuleId());
        }
        mockRule.setVersionId(matchRule.getVersionId() + 1);
        mockRule.setState(MatchRuleRecord.State.ACTIVE);
        return mockRule;
    }

    private List<MatchRule> getMockMatchRuleList(String sourceId) {
        MatchRule mockRule1 = new MatchRule();
        mockRule1.setSourceId(sourceId);
        mockRule1.setDisplayName("Match Rule 1");
        mockRule1.setRuleType(MatchRuleRecord.RuleType.BASE_RULE);

        mockRule1.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.OutOfBusiness));
        DplusMatchRule dplusMatchRule = new DplusMatchRule(8, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(4, 6, Arrays.asList("C", "D"));

        mockRule1.setAcceptCriterion(dplusMatchRule.getAcceptCriterion());


        mockRule1.setMatchRuleId(String.format("MatchRule_%s",
                RandomStringUtils.randomAlphanumeric(8).toLowerCase()));
        mockRule1.setVersionId(1);
        mockRule1.setState(MatchRuleRecord.State.ACTIVE);

        MatchRule mockRule2 = new MatchRule();
        mockRule2.setSourceId(sourceId);
        mockRule2.setDisplayName("Match Rule 2");
        mockRule2.setRuleType(MatchRuleRecord.RuleType.SPECIAL_RULE);
        mockRule2.setMatchKey(MatchKey.Country);
        mockRule2.setAllowedValues(Arrays.asList("USA", "UK"));
        mockRule2.setExclusionCriterionList(Arrays.asList(ExclusionCriterion.NonHeadQuarters, ExclusionCriterion.Unreachable));
        DplusMatchRule dplusMatchRule2 = new DplusMatchRule(4, 10, Arrays.asList("AAZZABB", "AAZZABF", "AAZZABZ"))
                .review(1, 10, Arrays.asList("C", "D"));

        mockRule2.setAcceptCriterion(dplusMatchRule2.getAcceptCriterion());

        mockRule2.setMatchRuleId(String.format("MatchRule_%s",
                RandomStringUtils.randomAlphanumeric(8).toLowerCase()));
        mockRule2.setVersionId(1);
        mockRule2.setState(MatchRuleRecord.State.ACTIVE);

        return Arrays.asList(mockRule1, mockRule2);
    }
}
