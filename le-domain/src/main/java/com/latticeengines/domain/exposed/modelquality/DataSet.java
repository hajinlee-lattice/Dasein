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
import javax.persistence.ManyToMany;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "MODELQUALITY_DATASET", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME" }) })
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class DataSet implements HasName, HasTenant, HasPid, Fact, Dimension {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    // There is no foreign key to the TENANT table because we want to
    // selectively add customer data sets
    @JsonProperty("customer_space")
    @Column(name = "CUSTOMER_SPACE", nullable = false)
    private String customerSpace;

    @Column(name = "NAME", unique = true, nullable = false)
    private String name;

    @Column(name = "INDUSTRY", nullable = false)
    private String industry;

    @Column(name = "TYPE", nullable = false)
    private DataSetType dataSetType;

    @Column(name = "SCHEMA_INTERPRETATION", nullable = false)
    private SchemaInterpretation schemaInterpretation;

    @JsonProperty("training_hdfs_path")
    @Column(name = "TRAINING_HDFS_PATH", length = 2048)
    private String trainingSetHdfsPath;

    @JsonProperty("test_hdfs_path")
    @Column(name = "TEST_HDFS_PATH")
    private String testSetHdfsPath;

    @JsonProperty("scoring_data_sets")
    @OneToMany(cascade = { CascadeType.ALL }, fetch = FetchType.EAGER, mappedBy = "dataSet")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private List<ScoringDataSet> scoringDataSets = new ArrayList<>();

    @ManyToMany(fetch = FetchType.LAZY, mappedBy = "dataSets", cascade = { CascadeType.MERGE, CascadeType.PERSIST,
            CascadeType.REFRESH, CascadeType.DETACH })
    @JsonIgnore
    private List<AnalyticTest> analyticTests = new ArrayList<>();

    @Override
    @MetricTag(tag = "DataSetName")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setTenant(Tenant tenant) {
        customerSpace = tenant.getId();
    }

    @Override
    public Tenant getTenant() {
        if (customerSpace == null) {
            throw new IllegalStateException("Customer space cannot be null.");
        }
        return new Tenant(customerSpace);
    }

    @MetricTag(tag = "DataSetIndustry")
    public String getIndustry() {
        return industry;
    }

    public void setIndustry(String industry) {
        this.industry = industry;
    }

    public DataSetType getDataSetType() {
        return dataSetType;
    }

    public void setDataSetType(DataSetType dataSetType) {
        this.dataSetType = dataSetType;
    }

    @MetricTag(tag = "DataSetType")
    @JsonIgnore
    public String getDataSetTypeStrValue() {
        return dataSetType.name();
    }

    public SchemaInterpretation getSchemaInterpretation() {
        return schemaInterpretation;
    }

    public void setSchemaInterpretation(SchemaInterpretation schemaInterpretation) {
        this.schemaInterpretation = schemaInterpretation;
    }

    @MetricTag(tag = "DataSetSchemaInterpretation")
    @JsonIgnore
    public String getSchemaInterpretationStrValue() {
        return schemaInterpretation.name();
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getTrainingSetHdfsPath() {
        return trainingSetHdfsPath;
    }

    public void setTrainingSetHdfsPath(String trainingSetHdfsPath) {
        this.trainingSetHdfsPath = trainingSetHdfsPath;
    }

    public String getTestSetHdfsPath() {
        return testSetHdfsPath;
    }

    public void setTestSetHdfsPath(String testSetHdfsPath) {
        this.testSetHdfsPath = testSetHdfsPath;
    }

    public List<ScoringDataSet> getScoringDataSets() {
        return scoringDataSets;
    }

    public void setScoringDataSets(List<ScoringDataSet> scoringDataSets) {
        this.scoringDataSets = scoringDataSets;
        for (ScoringDataSet scoringDataSet : scoringDataSets) {
            scoringDataSet.setDataSet(this);
        }
    }

    public void addScoringDataSet(ScoringDataSet scoringDataSet) {
        scoringDataSet.setDataSet(this);
        scoringDataSets.add(scoringDataSet);
    }

}
