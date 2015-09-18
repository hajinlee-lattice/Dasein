package com.latticeengines.domain.exposed.dataflow;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.Job;

@Entity
@Table(name = "DATAFLOW_JOB")
@PrimaryKeyJoinColumn(name = "JOB_PID")
public class DataFlowJob extends Job {

    private String customer;

    private String targetPath;
    
    private String dataFlowBeanName;

    @Column(name = "CUSTOMER")
    public String getCustomer() {
        return customer;
    }

    @Column(name = "CUSTOMER")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @Column(name = "TARGET_PATH")
    public String getTargetPath() {
        return targetPath;
    }

    @Column(name = "TARGET_PATH")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @Column(name = "DATAFLOW_BEAN_NAME")
    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Column(name = "DATAFLOW_BEAN_NAME")
    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }
}
