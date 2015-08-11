package com.latticeengines.release.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JenkinsJobParameters {
    List<NameValuePair> nameValuePairs = new ArrayList<>();

    @JsonProperty("parameter")
    public List<NameValuePair> getNameValuePairs() {
        return this.nameValuePairs;
    }

    @JsonProperty("parameter")
    public void setNameValuePairs(List<NameValuePair> nameValuePairs) {
        this.nameValuePairs = nameValuePairs;
    }

    public static class NameValuePair {
        private String name;

        private String value;

        public NameValuePair(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @JsonProperty("name")
        public String getName() {
            return this.name;
        }

        @JsonProperty("name")
        public void setName(String name) {
            this.name = name;
        }

        @JsonProperty("value")
        public String getValue() {
            return this.value;
        }

        @JsonProperty("value")
        public void setValue(String value) {
            this.value = value;
        }
    }
}