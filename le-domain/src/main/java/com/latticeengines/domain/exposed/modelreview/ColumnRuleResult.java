package com.latticeengines.domain.exposed.modelreview;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Lob;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;

@Entity
@javax.persistence.Table(name = "MODELREVIEW_COLUMNRESULT")
public class ColumnRuleResult extends BaseRuleResult {

    @JsonIgnore
    @Column(name = "FLAGGED_COLUMNS", nullable = true)
    @Lob
    @org.hibernate.annotations.Type(type = "org.hibernate.type.SerializableToBlobType")
    private List<String> flaggedColumnNames = new ArrayList<>();

    @JsonProperty
    @Transient
    private List<AttributeMetadata> flaggedColumns = new ArrayList<>();

    public List<String> getFlaggedColumnNames() {
        return flaggedColumnNames;
    }

    public void setFlaggedColumnNames(List<String> flaggedColumnNames) {
        this.flaggedColumnNames = flaggedColumnNames;
    }

    public List<AttributeMetadata> getFlaggedColumns() {
        return flaggedColumns;
    }

    public void setFlaggedColumns(List<AttributeMetadata> flaggedColumns) {
        this.flaggedColumns = flaggedColumns;
    }

}
