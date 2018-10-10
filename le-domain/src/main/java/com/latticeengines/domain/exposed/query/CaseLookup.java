package com.latticeengines.domain.exposed.query;

import java.util.Collection;
import java.util.Collections;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CaseLookup extends Lookup {

    @JsonProperty("alias")
    private String alias;

    @JsonProperty("cases")
    private TreeMap<String, Restriction> caseMap;

    @JsonProperty("default")
    private String defaultCase;

    // for jackson
    @SuppressWarnings("unused")
	private CaseLookup() {
    }

    public CaseLookup(TreeMap<String, Restriction> caseMap, String defaultCase, String alias) {
        this.caseMap = caseMap;
        this.defaultCase = defaultCase;
        this.alias = alias;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public TreeMap<String, Restriction> getCaseMap() {
        return caseMap;
    }

    public void setCaseMap(TreeMap<String, Restriction> caseMap) {
        this.caseMap = caseMap;
    }

    public String getDefaultCase() {
        return defaultCase;
    }

    public void setDefaultCase(String defaultCase) {
        this.defaultCase = defaultCase;
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        if (caseMap != null) {
            return caseMap.values();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
