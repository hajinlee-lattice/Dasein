package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "METADATA_TABLE_RELATIONSHIP")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableRelationship implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "SOURCE_CARDINALITY", nullable = false)
    @Enumerated(value = EnumType.ORDINAL)
    @JsonProperty("source_cardinality")
    private Cardinality sourceCardinality;

    @Column(name = "TARGET_CARDINALITY", nullable = false)
    @Enumerated(value = EnumType.ORDINAL)
    @JsonProperty("target_cardinality")
    private Cardinality targetCardinality;

    @Column(name = "SOURCE_ATTRIBUTES", nullable = true)
    @Type(type = "text")
    private String sourceAttributesString;

    @Column(name = "TARGET_ATTRIBUTES", nullable = true)
    @Type(type = "text")
    private String targetAttributesString;

    @Column(name = "TARGET_TABLE_NAME", nullable = false)
    @JsonProperty("target_table_name")
    private String targetTableName;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_SOURCE_TABLE_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table sourceTable;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Cardinality getSourceCardinality() {
        return sourceCardinality;
    }

    public void setSourceCardinality(Cardinality sourceCardinality) {
        this.sourceCardinality = sourceCardinality;
    }

    public Cardinality getTargetCardinality() {
        return targetCardinality;
    }

    public void setTargetCardinality(Cardinality targetCardinality) {
        this.targetCardinality = targetCardinality;
    }

    @JsonProperty("source_attributes")
    public List<String> getSourceAttributes() {
        if (sourceAttributesString == null) {
            return new ArrayList<>();
        }
        return Arrays.asList(sourceAttributesString.split(","));
    }

    public void setSourceAttributes(List<String> sourceAttributes) {
        this.sourceAttributesString = StringUtils.join(sourceAttributes, ",");
    }

    public void setSourceAttributes(String... sourceAttributes) {
        setSourceAttributes(Arrays.asList(sourceAttributes));
    }

    @JsonProperty("target_attributes")
    public List<String> getTargetAttributes() {
        if (targetAttributesString == null) {
            return new ArrayList<>();
        }
        return Arrays.asList(targetAttributesString.split(","));
    }

    public void setTargetAttributes(List<String> targetAttributes) {
        this.targetAttributesString = StringUtils.join(targetAttributes, ",");
    }

    public void setTargetAttributes(String... targetAttributes) {
        setTargetAttributes(Arrays.asList(targetAttributes));
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(Table sourceTable) {
        this.sourceTable = sourceTable;
    }
}
