package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_ANALYTIC_TEST")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class AnalyticTest implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("data_sets")
    @ManyToMany(cascade = { CascadeType.ALL })
    @JoinTable(name = "MODELQUALITY_AP_TEST_DATASET", //
    joinColumns = { @JoinColumn(name = "AP_TEST_ID") }, //
    inverseJoinColumns = { @JoinColumn(name = "DATASET_ID") })
    private List<DataSet> dataSets = new ArrayList<>();

    @JsonProperty("match_type")
    @Column(name = "MATCH_TYPE", nullable = false)
    private PropDataMatchType propDataMatchType;
    
    @JsonProperty("analytic_pipelines")
    @ManyToMany(cascade = { CascadeType.ALL })
    @JoinTable(name = "MODELQUALITY_AP_TEST_AP_PIPELINE", //
    joinColumns = { @JoinColumn(name = "AP_TEST_ID") }, //
    inverseJoinColumns = { @JoinColumn(name = "AP_PIPELINE_ID") })
    private List<AnalyticPipeline> analyticPipelines = new ArrayList<>();

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public List<DataSet> getDataSets() {
        return dataSets;
    }

    public void setDataSets(List<DataSet> dataSets) {
        this.dataSets = dataSets;
    }

    public PropDataMatchType getPropDataMatchType() {
        return propDataMatchType;
    }

    public void setPropDataMatchType(PropDataMatchType propDataMatchType) {
        this.propDataMatchType = propDataMatchType;
    }

    
}
