package com.latticeengines.domain.exposed.pls;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;

public class RatingRule {

    public static final String ACCOUNT_RULE = "accountRule";
    public static final String CONTACT_RULE = "contactRule";

    @JsonProperty("bucketToRuleMap")
    private TreeMap<String, Map<String, Restriction>> bucketToRuleMap = generateDefaultBuckets();

    @JsonProperty("defaultBucketName")
    private String defaultBucketName = RuleBucketName.C.getName();

    public RatingRule() {
    }

    public void setBucketToRuleMap(TreeMap<String, Map<String, Restriction>> bucketToRuleMap) {
        this.bucketToRuleMap = bucketToRuleMap;
    }

    public TreeMap<String, Map<String, Restriction>> getBucketToRuleMap() {
        return this.bucketToRuleMap;
    }

    public void setDefaultBucketName(String defaultBucketName) {
        this.defaultBucketName = defaultBucketName;
    }

    public String getDefaultBucketName() {
        return this.defaultBucketName;
    }

    private TreeMap<String, Map<String, Restriction>> generateDefaultBuckets() {
        TreeMap<String, Map<String, Restriction>> map = new TreeMap<>();
        map.put(RuleBucketName.A.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.A_MINUS.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.B.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.C.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.D.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.F.getName(), generateDefaultAccountAndContactBuckets());
        return map;
    }

    @SuppressWarnings({ "unused" })
    private Map<String, Restriction> generateDefaultAccountAndContactBuckets() {
        Map<String, Restriction> map = new HashMap<>();
        map.put(ACCOUNT_RULE, Restriction.builder().and(Collections.emptyList()).build());
        map.put(CONTACT_RULE, Restriction.builder().and(Collections.emptyList()).build());
        return map;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
