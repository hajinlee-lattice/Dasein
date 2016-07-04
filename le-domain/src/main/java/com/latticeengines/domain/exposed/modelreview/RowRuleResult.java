package com.latticeengines.domain.exposed.modelreview;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@javax.persistence.Table(name = "MODELREVIEW_ROWRESULT")
public class RowRuleResult extends BaseRuleResult {

    @JsonProperty
    @Column(name = "FLAGGED_ROW_TO_COLUMNS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, List<String>> flaggedRowIdAndColumnNames = new HashMap<>();

    @JsonProperty
    @Column(name = "FLAGGED_ROW_TO_POSITIVEEVENT", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private Map<String, Boolean> flaggedRowIdAndPositiveEvent = new HashMap<>();

    @JsonProperty
    @Transient
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

    public Map<String, Boolean> getFlaggedRowIdAndPositiveEvent() {
        return flaggedRowIdAndPositiveEvent;
    }

    public void setFlaggedRowIdAndPositiveEvent(Map<String, Boolean> flaggedRowIdAndPositiveEvent) {
        this.flaggedRowIdAndPositiveEvent = flaggedRowIdAndPositiveEvent;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((flaggedRowIdAndColumnNames == null) ? 0 : flaggedRowIdAndColumnNames.hashCode());
        result = prime * result
                + ((flaggedRowIdAndPositiveEvent == null) ? 0 : flaggedRowIdAndPositiveEvent.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        RowRuleResult other = (RowRuleResult) obj;
        if (flaggedRowIdAndColumnNames == null) {
            if (other.flaggedRowIdAndColumnNames != null)
                return false;
        } else if (!flaggedRowIdAndColumnNames.equals(other.flaggedRowIdAndColumnNames))
            return false;
        if (flaggedRowIdAndPositiveEvent == null) {
            if (other.flaggedRowIdAndPositiveEvent != null)
                return false;
        } else if (!flaggedRowIdAndPositiveEvent.equals(other.flaggedRowIdAndPositiveEvent))
            return false;
        return true;
    }

}
