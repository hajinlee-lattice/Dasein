package com.latticeengines.domain.exposed.modelreview;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;

import com.fasterxml.jackson.annotation.JsonProperty;

@Entity
@javax.persistence.Table(name = "MODELREVIEW_COLUMNRESULT")
public class ColumnRuleResult extends BaseRuleResult {

    @JsonProperty
    @Column(name = "FLAGGED_COLUMNS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private List<String> flaggedColumnNames = new ArrayList<>();

    public List<String> getFlaggedColumnNames() {
        return flaggedColumnNames;
    }

    public void setFlaggedColumnNames(List<String> flaggedColumnNames) {
        this.flaggedColumnNames = flaggedColumnNames;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((flaggedColumnNames == null) ? 0 : flaggedColumnNames.hashCode());
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
        ColumnRuleResult other = (ColumnRuleResult) obj;
        if (flaggedColumnNames == null) {
            if (other.flaggedColumnNames != null)
                return false;
        } else if (!flaggedColumnNames.equals(other.flaggedColumnNames))
            return false;
        return true;
    }

}
