package com.latticeengines.domain.exposed.dcp;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PurposeOfUse {

    private static final Set<String> DOMAIN_SET = ImmutableSet.of("D&B for Finance", "D&B for Supply",
            "D&B for Sales & Marketing", "D&B for Compliance", "D&B for Enterprise Master Data");

    private static final Set<String> RECORD_TYPE_SET = ImmutableSet.of("Domain Use", "Domain Master Data Use");

    @JsonProperty("domain")
    private String domain;

    @JsonProperty("recordType")
    private String recordType;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        if (StringUtils.isBlank(domain) || !DOMAIN_SET.contains(domain)) {
            throw new IllegalArgumentException("Domain should be one of: " + Strings.join(DOMAIN_SET, ","));
        }
        this.domain = domain;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        if (StringUtils.isBlank(recordType) || !RECORD_TYPE_SET.contains(recordType)) {
            throw new IllegalArgumentException("Record Type should be one of: " + Strings.join(RECORD_TYPE_SET, ","));
        }
        this.recordType = recordType;
    }
}
