package com.latticeengines.domain.exposed.cdl.activity;

import java.util.Collections;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

/*
 * wrapper for a set of keys
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class KeysWrapper {

    private final Set<String> keys;

    @JsonCreator
    public KeysWrapper(@JsonProperty("Keys") Set<String> keys) {
        this.keys = keys == null ? Collections.emptySet() : ImmutableSet.copyOf(keys);
    }

    @JsonProperty("Keys")
    public Set<String> getKeys() {
        return keys;
    }

    @Override
    public String toString() {
        return "KeysWrapperRequest{" + "keys=" + keys + '}';
    }
}
