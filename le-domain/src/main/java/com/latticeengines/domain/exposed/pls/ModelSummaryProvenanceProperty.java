package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.ManyToOne;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@Table(name = "MODEL_SUMMARY_PROVENANCE_PROPERTY")
public class ModelSummaryProvenanceProperty implements HasOptionAndValue, HasPid {

    private Long pid;
    private ModelSummary modelSummary;
    private String option;
    private String value;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @ManyToOne
    @JoinColumn(name = "MODEL_SUMMARY_ID", nullable = false)
    @JsonIgnore
    public ModelSummary getModelSummary() {
        return this.modelSummary;
    }

    @JsonIgnore
    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    @Column(name = "OPTION", nullable = false)
    @JsonProperty("option")
    @Index(name = "PROSPECT_DISCOVERY_OPTION_OPTION_IDX")
    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    @Column(name = "VALUE", nullable = true)
    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
