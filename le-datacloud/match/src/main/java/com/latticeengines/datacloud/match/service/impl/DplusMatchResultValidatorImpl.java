package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;

@Component("dplusMatchResultValidator")
public class DplusMatchResultValidatorImpl implements DnBMatchResultValidator {

    private static final Logger log = LoggerFactory.getLogger(DplusMatchResultValidatorImpl.class);

    public boolean validate(DnBMatchContext res) {
        if (StringUtils.isEmpty(res.getDuns())) {
            res.setDunsInAM(Boolean.FALSE);
        } else {
            res.setDunsInAM(true);
        }

        if (res.getDnbCode() != DnBReturnCode.OK || Boolean.TRUE.equals(res.getPatched())) {
            res.setACPassed(true);
            return true;
        }

        DplusMatchRule matchRule = res.getMatchRule();
        DnBMatchCandidate candidate = CollectionUtils.isNotEmpty(res.getCandidates()) ? res.getCandidates().get(0) : null;
        if (matchRule != null && candidate != null) {
            DnBMatchInsight insight = candidate.getMatchInsight();
            DplusMatchRule.ClassificationCriterion ac = matchRule.getAcceptCriterion();
            int confidenceCode = insight.getConfidenceCode();
            int lowest = ac.getLowestConfidenceCode();
            int highest = ac.getHighestConfidenceCode();
            if (confidenceCode >= lowest && confidenceCode <= highest) {
                res.setACPassed(true);
                return true;
            } else if (CollectionUtils.isNotEmpty(ac.getMatchGradePatterns())) {
                String matchGrade = insight.getMatchGrade().getRawCode();
                log.info("Checking match grade {}, because the confidence code {} is out of the range [{}, {}]", //
                        matchGrade, confidenceCode, lowest, highest);
                boolean match = ac.getCompiledMatchGradePatterns().stream() //
                        .anyMatch(pattern -> pattern.matcher(matchGrade).matches());
                if (match) {
                    res.setACPassed(true);
                    return true;
                }
            }
            res.setDnbCode(DnBReturnCode.DISCARD);
            res.setACPassed(false);
            return false;
        }
        res.setACPassed(true);
        return true;
    }

}
