package com.latticeengines.domain.exposed.dataflow;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CollectionTable;
import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.PrimaryKeyJoinColumn;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Entity
@javax.persistence.Table(name = "DATAFLOW_JOB")
@PrimaryKeyJoinColumn(name = "JOB_PID")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataFlowJob extends Job {

    private String customer;

    private String dataFlowBeanName;

    private String targetTableName;

    private List<String> sourceTableNames = new ArrayList<>();

    @Column(name = "CUSTOMER")
    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @Column(name = "DATAFLOW_BEAN_NAME")
    @JsonProperty("dataflow_bean_name")
    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }

    @Column(name = "TARGET_TABLE_NAME", nullable = true)
    @JsonProperty("target_table_name")
    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    @ElementCollection
    @CollectionTable(name = "DATAFLOW_JOB_SOURCE_TABLE", joinColumns = @JoinColumn(name = "FK_JOB_ID"))
    @Column(name = "TABLE_NAME")
    public List<String> getSourceTableNames() {
        return sourceTableNames;
    }

    public void setSourceTableNames(List<String> sourceTables) {
        this.sourceTableNames = sourceTables;
    }
}
