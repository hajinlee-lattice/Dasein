package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import org.codehaus.jackson.annotate.JsonProperty;

// Description: Configuration file for NumberOfContactsFlow transformation.
public class NumberOfContactsConfig extends TransformerConfig {
    // Table field to join the left side table (in this case Account).
    @JsonProperty("LhsJoinField")
    private String lhsJoinField;
    // Table field to join the right side table (in this case Contact).
    @JsonProperty("RhsJoinField")
    private String rhsJoinField;

    public String getLhsJoinField() {
        return lhsJoinField;
    }

    public void setLhsJoinField(String lhsJoinField) {
        this.lhsJoinField = lhsJoinField;
    }

    public String getRhsJoinField() {
        return rhsJoinField;
    }

    public void setRhsJoinField(String rhsJoinField) {
        this.rhsJoinField = rhsJoinField;
    }
}
