package com.latticeengines.domain.exposed.dcp;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.parquet.Strings;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PurposeOfUse {

    private static final Set<String> DOMAIN_SET = ImmutableSet.copyOf( //
            Arrays.stream(DataDomain.values()).map(DataDomain::getDisplayName).collect(Collectors.toSet()) //
    );

    private static final Set<String> RECORD_TYPE_SET = ImmutableSet.copyOf( //
            Arrays.stream(DataRecordType.values()) //
                    .filter(drt -> !DataRecordType.Analytical.equals(drt)) // not support Analytical in DCP for now
                    .map(DataRecordType::getDisplayName).collect(Collectors.toSet()) //
    );

    @JsonProperty("domain")
    private String domain;

    @JsonProperty("recordType")
    private String recordType;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        if (StringUtils.isBlank(domain) || !DOMAIN_SET.contains(domain)) {
            throw new IllegalArgumentException("Domain should be one of: " + Strings.join(DOMAIN_SET, ", "));
        }
        this.domain = domain;
    }

    public String getRecordType() {
        return recordType;
    }

    public void setRecordType(String recordType) {
        if (StringUtils.isBlank(recordType) || !RECORD_TYPE_SET.contains(recordType)) {
            throw new IllegalArgumentException("Record Type should be one of: " + Strings.join(RECORD_TYPE_SET, ", "));
        }
        this.recordType = recordType;
    }
}
