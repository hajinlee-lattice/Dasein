package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Table(name = "MODEL_FEATURE_IMPORTANCE")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class ModelFeatureImportance implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @OneToOne
    @JoinColumn(name = "FK_MODELSUMMARY_ID", nullable = false)
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private ModelSummary modelSummary;

    @JsonProperty("column_name")
    @Column(name = "COLUMN_NAME", nullable = false)
    private String columnName;

    @JsonProperty("feature_importance")
    @Column(name = "FEATURE_IMPORTANCE", nullable = false)
    private double featureImportance;

    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME", nullable = true)
    private String displayName;

    public ModelFeatureImportance() {
    }

    public ModelFeatureImportance(ModelSummary modelSummary, String columnName, double featureImportance) {
        super();
        this.modelSummary = modelSummary;
        this.columnName = columnName;
        this.featureImportance = featureImportance;
    }

    public ModelFeatureImportance(ModelSummary modelSummary, String columnName, double featureImportance,
            String displayName) {
        super();
        this.modelSummary = modelSummary;
        this.columnName = columnName;
        this.featureImportance = featureImportance;
        this.displayName = displayName;
    }


    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public double getFeatureImportance() {
        return featureImportance;
    }

    public void setFeatureImportance(double featureImportance) {
        this.featureImportance = featureImportance;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
