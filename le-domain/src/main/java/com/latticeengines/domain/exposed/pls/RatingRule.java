package com.latticeengines.domain.exposed.pls;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;

public class RatingRule {

    public static final String DEFAULT_BUCKET_NAME = RatingBucketName.A.getName();

    @JsonProperty("bucketToRuleMap")
    private TreeMap<String, Map<String, Restriction>> bucketToRuleMap = new TreeMap<>();

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

    @JsonIgnore
    public void setRuleForBucket(RatingBucketName bucket, Restriction accountRestriction, Restriction contactRestriction) {
        if (accountRestriction == null && contactRestriction == null) {
            return;
        }
        Map<String, Restriction> rules = new HashMap<>();
        if (accountRestriction != null) {
            rules.put(FrontEndQueryConstants.ACCOUNT_RESTRICTION, accountRestriction);
        }
        if (contactRestriction != null) {
            rules.put(FrontEndQueryConstants.CONTACT_RESTRICTION, contactRestriction);
        }
        bucketToRuleMap.put(bucket.getName(), rules);
    }

    @JsonIgnore
    public Map<String, Restriction> getRuleForBucket(RatingBucketName bucket) {
        return bucketToRuleMap.get(bucket.getName());
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
        map.put(RatingBucketName.A.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RatingBucketName.B.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RatingBucketName.C.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RatingBucketName.D.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RatingBucketName.F.getName(), generateDefaultAccountAndContactBuckets());
        map.put(RatingBucketName.G.getName(), generateDefaultAccountAndContactBuckets());
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
