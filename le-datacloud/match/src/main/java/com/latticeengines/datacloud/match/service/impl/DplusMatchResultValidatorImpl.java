package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Classification.Accepted;
import static com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate.Classification.Rejected;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchInsight;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchRule;
import com.latticeengines.domain.exposed.datacloud.match.config.ExclusionCriterion;

@Component("dplusMatchResultValidator")
public class DplusMatchResultValidatorImpl implements DnBMatchResultValidator {

    private static final Logger log = LoggerFactory.getLogger(DplusMatchResultValidatorImpl.class);

    @Value("${datacloud.dnb.realtime.operatingstatus.outofbusiness}")
    private String outOfBusinessValue;

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

        DnBMatchCandidate candidate = CollectionUtils.isNotEmpty(res.getCandidates()) ? res.getCandidates().get(0) : null;
        if (candidate != null) {
            DnBMatchCandidate.Classification classification = Accepted;
            DplusMatchRule matchRule = res.getMatchRule();
            if (matchRule != null) {
                classification = classifyCandidate(candidate, matchRule);
            }
            candidate.setClassification(classification);
            if (Accepted.equals(classification)) {
                res.setACPassed(true);
                return true;
            } else {
                res.setDnbCode(DnBReturnCode.DISCARD);
                res.setACPassed(false);
                return false;
            }
        }
        res.setACPassed(true);
        return true;
    }

    private DnBMatchCandidate.Classification classifyCandidate(DnBMatchCandidate candidate, DplusMatchRule matchRule) {
        DnBMatchCandidate.Classification classification = Rejected;
        // exclusion criteria
        boolean shouldExclude = false;
        List<ExclusionCriterion> criteria = new ArrayList<>(matchRule.getExclusionCriteria());
        if (CollectionUtils.isNotEmpty(criteria)) {
            for (ExclusionCriterion criterion: criteria) {
                if (shouldExclude(candidate, criterion)) {
                    log.warn("Should exclude candidate with duns={} by criterion={}", candidate.getDuns(), criterion);
                    shouldExclude = true;
                }
            }
        }
        if (shouldExclude) {
            return Rejected;
        }

        // other match rules
        DnBMatchInsight insight = candidate.getMatchInsight();
        DplusMatchRule.ClassificationCriterion ac = matchRule.getAcceptCriterion();
        int confidenceCode = insight.getConfidenceCode();
        int lowest = ac.getLowestConfidenceCode();
        int highest = ac.getHighestConfidenceCode();
        if (confidenceCode >= lowest && confidenceCode <= highest) {
            candidate.setClassification(Accepted);
            classification = Accepted;
        } else if (CollectionUtils.isNotEmpty(ac.getMatchGradePatterns())) {
            String matchGrade = insight.getMatchGrade().getRawCode();
            log.info("Checking match grade {}, because the confidence code {} is out of the range [{}, {}]", //
                    matchGrade, confidenceCode, lowest, highest);
            boolean match = ac.getCompiledMatchGradePatterns().stream() //
                    .anyMatch(pattern -> pattern.matcher(matchGrade).matches());
            if (match) {
                classification = Accepted;
            }
        }
        return classification;
    }

    /**
     * * ExcludeNonHeadQuarters: Only entities that are headquarters are returned. A headquarter is defined as Parent/HQ, Domestic Ultimate, or Global Ultimate.
     * * ExcludeNonMarketable: Only entities where isMarketable = true are returned.
     * * ExcludeOutofBusiness: Only entities where isOutofBusiness = false are returned.
     * * ExcludeUndeliverable: Only entities where isMailUndeliverable = false are returned.
     * * ExcludeUnreachable: Only entites where isUnreachable = false are returned.
    **/
    private boolean shouldExclude(DnBMatchCandidate candidate, ExclusionCriterion criterion) {
        boolean shouldExclude;
        switch (criterion) {
            case OutOfBusiness:
                shouldExclude = outOfBusinessValue.equalsIgnoreCase(candidate.getOperatingStatus());
                break;
            case Unreachable:
                shouldExclude =  Boolean.TRUE.equals(candidate.getUnreachable());
                break;
            case Undeliverable:
                shouldExclude =  Boolean.TRUE.equals(candidate.getMailUndeliverable());
                break;
            case NonHeadQuarters:
                List<String> roles = candidate.getFamilyTreeRoles();
                if (CollectionUtils.isNotEmpty(roles)) {
                    boolean isHeadQuarter = false;
                    for (String role: roles) {
                        String role2 = role.toLowerCase();
                        isHeadQuarter = role2.contains("parent") || role2.contains("ultimate");
                    }
                    shouldExclude = !isHeadQuarter; // exclude non head quarter entities
                } else {
                    shouldExclude =  false; // standalone entity: always head quarter
                }
                break;
            case NonMarketable:
                // FIXME: (DCP-2144) not sure how to parse isMarketable
            default:
                shouldExclude = false;
        }
        return shouldExclude;
    }

}
