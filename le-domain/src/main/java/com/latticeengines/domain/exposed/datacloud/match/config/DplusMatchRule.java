package com.latticeengines.domain.exposed.datacloud.match.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class DplusMatchRule {

    public static final int LOWEST_CODE = 1;
    public static final int HIGHEST_CODE = 10;

    @JsonProperty("ExclusionCriteria")
    private Collection<ExclusionCriterion> exclusionCriteria;

    @JsonProperty("AcceptCriterion")
    private ClassificationCriterion acceptCriterion;

    @JsonProperty("ReviewCriterion")
    private ClassificationCriterion reviewCriterion;

    public DplusMatchRule(){
        this(LOWEST_CODE);
    }

    public DplusMatchRule(int lowCode) {
        this(lowCode, HIGHEST_CODE);
    }

    public DplusMatchRule(int lowCode, Collection<String> gradePatterns) {
        this(lowCode, HIGHEST_CODE, gradePatterns);
    }

    public DplusMatchRule(int lowCode, int highCode) {
        this(lowCode, highCode, null);
    }

    public DplusMatchRule(int lowCode, int highCode, Collection<String> gradePatterns) {
        this.acceptCriterion = new ClassificationCriterion(lowCode, highCode, gradePatterns);
    }

    public Collection<ExclusionCriterion> getExclusionCriteria() {
        return exclusionCriteria;
    }

    private void setExclusionCriteria(Collection<ExclusionCriterion> exclusionCriteria) {
        this.exclusionCriteria = exclusionCriteria;
    }

    public ClassificationCriterion getAcceptCriterion() {
        return acceptCriterion;
    }

    private void setAcceptCriterion(ClassificationCriterion acceptCriterion) {
        this.acceptCriterion = acceptCriterion;
    }

    public ClassificationCriterion getReviewCriterion() {
        return reviewCriterion;
    }

    private void setReviewCriterion(ClassificationCriterion reviewCriterion) {
        this.reviewCriterion = reviewCriterion;
    }

    // can be called multiple times. append to exclusionCriteria collection
    public DplusMatchRule exclude(ExclusionCriterion... criteria) {
        ExclusionCriterion first = criteria[0];
        EnumSet<ExclusionCriterion> criteriaSet = criteria.length > 1 ? EnumSet.of(first) : //
                EnumSet.of(first, Arrays.copyOfRange(criteria, 1, criteria.length));
        if (CollectionUtils.isNotEmpty(exclusionCriteria)) {
            criteriaSet.addAll(exclusionCriteria);
        }
        this.exclusionCriteria = criteriaSet;
        return this;
    }

    public DplusMatchRule accept(int lowCode) {
        return accept(lowCode, HIGHEST_CODE);
    }

    public DplusMatchRule accept(int lowCode, Collection<String> gradePatterns) {
        return accept(lowCode, HIGHEST_CODE, gradePatterns);
    }

    public DplusMatchRule accept(int lowCode, int highCode) {
        return accept(lowCode, highCode, Collections.emptyList());
    }

    // can be called multiple times. overwrite previous setting
    public DplusMatchRule accept(int lowCode, int highCode, Collection<String> gradePatterns) {
        this.acceptCriterion = new ClassificationCriterion(lowCode, highCode, gradePatterns);
        return this;
    }

    public DplusMatchRule review(int lowCode) {
        return accept(lowCode, 10);
    }

    public DplusMatchRule review(int lowCode, int highCode) {
        return accept(lowCode, highCode, Collections.emptyList());
    }

    // can be called multiple times. overwrite previous setting
    public DplusMatchRule review(int lowCode, int highCode, Collection<String> gradePatterns) {
        this.reviewCriterion = new ClassificationCriterion(lowCode, highCode, gradePatterns);
        return this;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect( //
            fieldVisibility = JsonAutoDetect.Visibility.NONE, //
            getterVisibility = JsonAutoDetect.Visibility.NONE, //
            isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
            setterVisibility = JsonAutoDetect.Visibility.NONE //
    )
    public static class ClassificationCriterion {

        @JsonProperty("LowestConfidenceCode")
        private int lowestConfidenceCode;

        @JsonProperty("HighestConfidenceCode")
        private int highestConfidenceCode;

        @JsonProperty("MatchGradePatterns")
        private Collection<String> matchGradePatterns;

        // for jackson
        private ClassificationCriterion(){}

        private ClassificationCriterion(int lowCode, int highCode, Collection<String> gradePatterns) {
            this.lowestConfidenceCode = lowCode;
            this.highestConfidenceCode = highCode;
            if (CollectionUtils.isNotEmpty(gradePatterns)) {
                this.matchGradePatterns = new ArrayList<>(gradePatterns); // copy
            }
        }

        public int getLowestConfidenceCode() {
            return lowestConfidenceCode;
        }

        public void setLowestConfidenceCode(int lowestConfidenceCode) {
            this.lowestConfidenceCode = lowestConfidenceCode;
        }

        public int getHighestConfidenceCode() {
            return highestConfidenceCode;
        }

        public void setHighestConfidenceCode(int highestConfidenceCode) {
            this.highestConfidenceCode = highestConfidenceCode;
        }

        public Collection<String> getMatchGradePatterns() {
            return matchGradePatterns;
        }

        public void setMatchGradePatterns(Collection<String> matchGradePatterns) {
            this.matchGradePatterns = matchGradePatterns;
        }

    }
}
