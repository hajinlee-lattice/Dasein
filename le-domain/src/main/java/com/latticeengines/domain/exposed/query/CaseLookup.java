package com.latticeengines.domain.exposed.query;

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RuleBucketName;

import edu.emory.mathcs.backport.java.util.Collections;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CaseLookup extends Lookup {

    @JsonProperty("bucketToRuleMap")
    private TreeMap<String, Restriction> bucketToRuleMap = generateDefaultBuckets();

    @JsonProperty("defaultBucketName")
    private String defaultBucketName = RuleBucketName.C.getName();

    public CaseLookup() {
    }

    public void setBucketToRuleMap(Map<String, Restriction> bucketToRuleMap) {
        for (Map.Entry<String, Restriction> entry : bucketToRuleMap.entrySet()) {
            this.addToMap(entry);
        }
    }

    public void addToMap(Map.Entry<String, Restriction> entry) {
        if (this.bucketToRuleMap == null) {
            this.bucketToRuleMap = new TreeMap<>();
        }
        this.bucketToRuleMap.put(entry.getKey(), entry.getValue());
    }

    public void addToMap(String key, Restriction value) {
        if (this.bucketToRuleMap == null) {
            this.bucketToRuleMap = new TreeMap<>();
        }
        this.bucketToRuleMap.put(key, value);
    }

    public Map<String, Restriction> getBucketToRuleMap() {
        return this.bucketToRuleMap;
    }

    public void setDefaultBucketName(String defaultBucketName) {
        this.defaultBucketName = defaultBucketName;
    }

    public String getDefaultBucketName() {
        return this.defaultBucketName;
    }

    @SuppressWarnings({ "unchecked", "unused" })
    private TreeMap<String, Restriction> generateDefaultBuckets() {
        TreeMap<String, Restriction> map = new TreeMap<>();
        map.put(RuleBucketName.A.getName(), Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        map.put(RuleBucketName.A_MINUS.getName(),
                Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        map.put(RuleBucketName.B.getName(), Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        map.put(RuleBucketName.C.getName(), Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        map.put(RuleBucketName.D.getName(), Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        map.put(RuleBucketName.F.getName(), Restriction.builder().and(Collections.<Restriction> emptyList()).build());
        return map;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
