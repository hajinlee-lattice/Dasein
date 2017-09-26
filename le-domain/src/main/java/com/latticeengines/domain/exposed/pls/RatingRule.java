package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;

public class RatingRule {

    public static final String DEFAULT_BUCKET_NAME = RuleBucketName.C.getName();

    @JsonProperty("bucketToRuleMap")
    private TreeMap<String, Map<String, Restriction>> bucketToRuleMap;

    @JsonProperty("defaultBucketName")
    private String defaultBucketName = DEFAULT_BUCKET_NAME;

    public static RatingRule constructDefaultRule() {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setBucketToRuleMap(generateDefaultBuckets());
        return ratingRule;
    }

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

    @VisibleForTesting
    static TreeMap<String, Map<String, Restriction>> generateDefaultBuckets() {
        TreeMap<String, Map<String, Restriction>> map = new TreeMap<>();
        map.put(RuleBucketName.A.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.A_MINUS.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.B.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.C.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.D.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RuleBucketName.F.getName(), generateDefaultAccountAndContactBuckets());
        return map;
    }

    private static Map<String, Restriction> generateDefaultAccountAndContactBuckets() {
        Map<String, Restriction> map = new HashMap<>();
        map.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, null);
        map.put(FrontEndQueryConstants.CONTACT_RESTRICTION, null);
        return map;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
