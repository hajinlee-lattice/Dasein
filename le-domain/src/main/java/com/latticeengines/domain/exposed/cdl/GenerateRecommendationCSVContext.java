package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public class GenerateRecommendationCSVContext implements Serializable {

    private static final long serialVersionUID = 410242962845998409L;

    public GenerateRecommendationCSVContext() {
        super();
    }

    @JsonProperty("IgnoreAccountsWithoutContacts")
    private boolean ignoreAccountsWithoutContacts;

    @JsonProperty("LiveRamp")
    private boolean liveRamp;

    @JsonProperty("Fields")
    private List<String> fields;

    @JsonProperty("DisplayNames")
    private Map<String, String> displayNames;

    public boolean isIgnoreAccountsWithoutContacts() {
        return ignoreAccountsWithoutContacts;
    }

    public void setIgnoreAccountsWithoutContacts(boolean ignoreAccountsWithoutContacts) {
        this.ignoreAccountsWithoutContacts = ignoreAccountsWithoutContacts;
    }

    public boolean isLiveRamp() {
        return liveRamp;
    }

    public void setLiveRamp(boolean liveRamp) {
        this.liveRamp = liveRamp;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public Map<String, String> getDisplayNames() {
        return displayNames;
    }

    public void setDisplayNames(Map<String, String> displayNames) {
        this.displayNames = displayNames;
    }
}