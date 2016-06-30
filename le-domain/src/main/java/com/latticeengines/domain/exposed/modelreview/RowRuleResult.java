package com.latticeengines.domain.exposed.modelreview;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;

import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@javax.persistence.Table(name = "MODELREVIEW_ROWRESULT")
public class RowRuleResult extends BaseRuleResult {

    @JsonProperty
    @Column(name = "FLAGGED_ROWS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, List<String>> flaggedRowIdAndColumnNames = new HashMap<>();

    @JsonProperty
    @Column(name = "NUM_EVENTS", nullable = false)
    private int numPositiveEvents = 0;

    public Map<String, List<String>> getFlaggedRowIdAndColumnNames() {
        return flaggedRowIdAndColumnNames;
    }

    public void setFlaggedRowIdAndColumnNames(Map<String, List<String>> flaggedRowIdAndColumnNames) {
        this.flaggedRowIdAndColumnNames = flaggedRowIdAndColumnNames;
    }

    public int getNumPositiveEvents() {
        return numPositiveEvents;
    }

    public void setNumPositiveEvents(int numPositiveEvents) {
        this.numPositiveEvents = numPositiveEvents;
    }
}
