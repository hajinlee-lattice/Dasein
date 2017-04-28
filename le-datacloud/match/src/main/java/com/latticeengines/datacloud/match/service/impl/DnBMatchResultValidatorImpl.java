package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchGrade;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("dnbMatchResultValidatorImpl")
public class DnBMatchResultValidatorImpl implements DnBMatchResultValidator {

    private static final Log log = LogFactory.getLog(DnBMatchResultValidatorImpl.class);

    @Value("${datacloud.dnb.matchcriteria:default}")
    private String matchCriteriaJson;

    private DnBMatchCriteria matchCriteria;

    @PostConstruct
    public void loadMatchCriteria() {

        try {
            if (!matchCriteriaJson.equals("default")) {
                matchCriteria = JsonUtils.deserialize(matchCriteriaJson, DnBMatchCriteria.class);
            }
        } catch (Exception e) {
            matchCriteria = null;
        }

        if (matchCriteria == null) {

            matchCriteria = new DnBMatchCriteria();

            DnBMatchBucket bucket;

            bucket  = new DnBMatchBucket();
            bucket.setLowerConfidence(8);
            bucket.setUpperConfidence(10);
            bucket.addRule("*", "*", "*", "*");
            matchCriteria.addBucket(bucket);

            bucket  = new DnBMatchBucket();
            bucket.setLowerConfidence(6);
            bucket.setUpperConfidence(7);
            bucket.addRule("A", "*", "*", "A");
            bucket.addRule("A", "ABZ", "ABZ", "BZ");
            bucket.addRule("B", "ABZ", "AZ", "ABZ");
            bucket.addRule("AB", "A", "A", "F");
            bucket.addRule("A", "F", "A", "AB");
            matchCriteria.addBucket(bucket);

            bucket  = new DnBMatchBucket();
            bucket.setLowerConfidence(4);
            bucket.setUpperConfidence(5);
            bucket.addRule("A", "A", "AB", "ABZ");
            bucket.addRule("A", "B", "A", "ABZ");
            bucket.addRule("A", "AZ", "A", "ABZ");
            matchCriteria.addBucket(bucket);
        }

        log.info("Dnb Match Criteria: " + JsonUtils.serialize(matchCriteria));
    }

    public boolean validate(DnBMatchContext res) {
        if (res.getDnbCode() != DnBReturnCode.OK || Boolean.TRUE.equals(res.getPatched())) {
            res.setPassAcceptanceCriteria(true);
            return true;
        }

        if (Boolean.TRUE.equals(res.isOutOfBusiness()) || Boolean.FALSE.equals(res.isDunsInAM())) {
            res.setDnbCode(DnBReturnCode.DISCARD);
            res.setPassAcceptanceCriteria(false);
            return false;
        }

        if (!matchCriteria.accept(res.getConfidenceCode(), res.getMatchGrade())) {
            res.setDnbCode(DnBReturnCode.DISCARD);
            res.setPassAcceptanceCriteria(false);
            return false;
        }
        res.setPassAcceptanceCriteria(true);
        return true;
    }

    public static class DnBMatchCriteria {

        @JsonProperty("Buckets")
        List<DnBMatchBucket> buckets;

        public boolean accept(Integer confidence, DnBMatchGrade matchGrade) {
            if ((confidence == null) || (matchGrade == null) || (buckets == null)) {
                return false;
            }
            for (DnBMatchBucket bucket : buckets) {
                if (bucket.accept(confidence, matchGrade)) {
                    return true;
                }
            }
            return false;
        }

        public List<DnBMatchBucket> getBuckets() {
            return buckets;
        }

        public void setBuckets(List<DnBMatchBucket> buckets) {
            this.buckets = buckets;
        }

        public void addBucket(DnBMatchBucket bucket) {
            if (buckets == null) {
                buckets = new ArrayList<DnBMatchBucket>();
            }
            buckets.add(bucket);
        }
    }

    public static class DnBMatchBucket {

        @JsonProperty("LowerConfidence")
        private Integer lowerConfidence;

        @JsonProperty("UpperConfidence")
        private Integer upperConfidence;

        @JsonProperty("Rules")
        private List<DnBMatchRule> rules;

        public void setUpperConfidence(Integer upperConfidence) {
            this.upperConfidence = upperConfidence;
        }

        public Integer getUpperConfidence() {
            return upperConfidence;
        }

        public void setLowerConfidence(Integer lowerConfidence) {
            this.lowerConfidence = lowerConfidence;
        }

        public Integer getLowerConfidence() {
            return lowerConfidence;
        }

        public List<DnBMatchRule> getRules() {
            return rules;
        }

        public void setRules(List<DnBMatchRule> rules) {
            this.rules = rules;
        }

        public void addRule(String name, String city, String state, String zip) {
            DnBMatchRule rule = new DnBMatchRule();
            rule.setName(name);
            rule.setCity(city);
            rule.setState(state);
            rule.setZip(zip);
            if (rules == null) {
                rules = new ArrayList<DnBMatchRule>();
            }
            rules.add(rule);
        }

        public boolean accept(Integer confidence, DnBMatchGrade matchGrade) {
            if ((confidence < lowerConfidence) || (confidence > upperConfidence)) {
                return false;
            } else {
                if (rules == null) {
                    return false;
                }
                for (DnBMatchRule rule : rules) {
                    if (rule.accept(matchGrade)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public static class DnBMatchRule {

        @JsonProperty("Name")
        private String name;

        @JsonProperty("City")
        private String city;

        @JsonProperty("State")
        private String state;

        @JsonProperty("Zip")
        private String zip;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getZip() {
            return zip;
        }

        public void setZip(String zip) {
            this.zip = zip;
        }


        public boolean accept(DnBMatchGrade matchGrade) {
            return (accept(name, matchGrade.getNameCode()) &&
                    accept(city, matchGrade.getCityCode()) &&
                    accept(state, matchGrade.getStateCode()) &&
                    accept(zip, matchGrade.getZipCodeCode()));
        }

        private boolean accept(String rule, String grade) {
             if (rule.equals("*")) {
                 return true;
             } else {
                 grade = (grade == null) ? "Z" : grade;
                 if (rule.contains(grade)) {
                     return true;

                 }
             }
             return false;
        }
    }

}
