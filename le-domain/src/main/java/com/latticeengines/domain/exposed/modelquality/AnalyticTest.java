package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_ANALYTIC_TEST", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class AnalyticTest implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", unique = true, nullable = false)
    private String name;

    @JsonProperty("data_sets")
    @ManyToMany(fetch=FetchType.EAGER, cascade = { CascadeType.MERGE })
    @JoinTable(name = "MODELQUALITY_AP_TEST_DATASET", joinColumns = {
            @JoinColumn(name = "AP_TEST_ID") }, inverseJoinColumns = { @JoinColumn(name = "DATASET_ID") })
    @Fetch(value=FetchMode.SUBSELECT)
    private List<DataSet> dataSets = new ArrayList<>();

    @JsonProperty("match_type")
    @Column(name = "MATCH_TYPE", nullable = false)
    private PropDataMatchType propDataMatchType;

    @JsonProperty("analytic_pipelines")
    @ManyToMany(fetch=FetchType.EAGER, cascade = { CascadeType.MERGE })
    @JoinTable(name = "MODELQUALITY_AP_TEST_AP_PIPELINE", 
               joinColumns = { @JoinColumn(name = "AP_TEST_ID") }, 
               inverseJoinColumns = { @JoinColumn(name = "AP_PIPELINE_ID") })
    @Fetch(value=FetchMode.SUBSELECT)
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

    public List<AnalyticPipeline> getAnalyticPipelines() {
        return this.analyticPipelines;
    }

    public void setAnalyticPipelines(List<AnalyticPipeline> analyticPipelines) {
        this.analyticPipelines = analyticPipelines;
    }

}
