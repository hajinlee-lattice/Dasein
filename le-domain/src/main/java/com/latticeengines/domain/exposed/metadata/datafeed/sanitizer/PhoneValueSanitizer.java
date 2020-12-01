package com.latticeengines.domain.exposed.metadata.datafeed.sanitizer;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PhoneValueSanitizer extends InputValueSanitizer {

    private static final int SANITIZER_ORDER = 10;

    private static final Set<String> PHONE_COLUMNS = ImmutableSet.of(InterfaceName.PhoneNumber.name(),
            InterfaceName.SecondaryPhoneNumber.name(), InterfaceName.OtherPhoneNumber.name());

    @JsonProperty("trim")
    private Boolean trim;

    public Boolean isTrim() {
        return trim;
    }

    public void setTrim(Boolean trim) {
        this.trim = trim;
    }

    @Override
    public int getOrder() {
        return SANITIZER_ORDER;
    }

    @Override
    public String sanitize(String input, Attribute attribute) {
        if (attribute == null) {
            return input;
        }
        if (PHONE_COLUMNS.contains(attribute.getName())) {
            if (StringUtils.isEmpty(input)) {
                return input;
            }
            String output = Boolean.TRUE.equals(trim) ? input.trim() : input;
            if ("NULL".equalsIgnoreCase(output)) {
                output = null;
            }
            return output;
        } else {
            return input;
        }
    }
}
