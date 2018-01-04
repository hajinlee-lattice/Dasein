package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
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

    @JsonProperty("analytic_test_type")
    @Column(name = "ANALYTIC_TEST_TYPE", nullable = false)
    @Enumerated(EnumType.ORDINAL)
    private AnalyticTestType analyticTestType = AnalyticTestType.SelectedPipelines;

    @JsonProperty("analytic_test_tag")
    @Column(name = "ANALYTIC_TEST_TAG")
    private String analyticTestTag;

    @JsonProperty("is_executed")
    @Column(name = "IS_EXECUTED", nullable = false)
    private boolean isExecuted;

    @JsonProperty("data_sets")
    @ManyToMany(fetch = FetchType.EAGER, cascade = { CascadeType.MERGE })
    @JoinTable(name = "MODELQUALITY_AP_TEST_DATASET", joinColumns = {
            @JoinColumn(name = "AP_TEST_ID") }, inverseJoinColumns = { @JoinColumn(name = "DATASET_ID") })
    @Fetch(value = FetchMode.SUBSELECT)
    private List<DataSet> dataSets = new ArrayList<>();

    @JsonProperty("analytic_pipelines")
    @ManyToMany(fetch = FetchType.EAGER, cascade = { CascadeType.MERGE })
    @JoinTable(name = "MODELQUALITY_AP_TEST_AP_PIPELINE", joinColumns = {
            @JoinColumn(name = "AP_TEST_ID") }, inverseJoinColumns = { @JoinColumn(name = "AP_PIPELINE_ID") })
    @Fetch(value = FetchMode.SUBSELECT)
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

    public AnalyticTestType getAnalyticTestType() {
        return analyticTestType;
    }

    public void setAnalyticTestType(AnalyticTestType analyticTestType) {
        this.analyticTestType = analyticTestType;
    }

    public String getAnalyticTestTag() {
        return analyticTestTag;
    }

    public void setAnalyticTestTag(String analyticTestTag) {
        this.analyticTestTag = analyticTestTag;
    }

    public boolean isExecuted() {
        return isExecuted;
    }

    public void setExecuted(boolean isExecuted) {
        this.isExecuted = isExecuted;
    }

    public List<AnalyticPipeline> getAnalyticPipelines() {
        return this.analyticPipelines;
    }

    public void setAnalyticPipelines(List<AnalyticPipeline> analyticPipelines) {
        this.analyticPipelines = analyticPipelines;
    }

    public List<DataSet> getDataSets() {
        return dataSets;
    }

    public void setDataSets(List<DataSet> dataSets) {
        this.dataSets = dataSets;
    }
}
