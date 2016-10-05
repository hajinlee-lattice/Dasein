package com.latticeengines.domain.exposed.modelquality;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_SCORING_DATASET")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class ScoringDataSet implements HasName, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", unique = true, nullable = false)
    private String name;

    @JsonProperty("data_hdfs_path")
    @Column(name = "DATA_PATH")
    private String dataHdfsPath;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_DATASET_ID", nullable = false)
    private DataSet dataSet;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getDataHdfsPath() {
        return dataHdfsPath;
    }

    public void setDataHdfsPath(String dataHdfsPath) {
        this.dataHdfsPath = dataHdfsPath;
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

}
