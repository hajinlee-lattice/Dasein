package com.latticeengines.domain.exposed.modelquality;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_PIPELINE_PIPELINE_STEP")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class PipelineToPipelineSteps implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @Column(name = "STEP_ORDER", nullable = true)
    private int order = -1;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }


}
