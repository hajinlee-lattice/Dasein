package com.latticeengines.domain.exposed.modelquality;

import java.io.Serializable;

import javax.persistence.AssociationOverride;
import javax.persistence.AssociationOverrides;
import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;


@Entity
@Table(name = "MODELQUALITY_PIPELINE_PIPELINE_STEP")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
@AssociationOverrides({
    @AssociationOverride(name = "pk.pipeline",
        joinColumns = @JoinColumn(name = "PIPELINE_ID")),
    @AssociationOverride(name = "pk.pipelineStep",
        joinColumns = @JoinColumn(name = "PIPELINE_STEP_ID")) })
public class PipelineToPipelineSteps implements HasPid, Serializable {

    private static final long serialVersionUID = -6573046838725282391L;

    @Column(name = "STEP_ORDER", nullable = true)
    private Integer order = -1;
    
    @EmbeddedId
    private PipelineToPipelineStepsId pk = new PipelineToPipelineStepsId();
    
    public Integer getOrder() {
        return order;
    }

    public void setOrder(Integer order) {
        this.order = order;
    }

    @Transient
    public Pipeline getPipeline() {
        return pk.getPipeline();
    }

    public void setPipeline(Pipeline pipeline) {
        pk.setPipeline(pipeline);
    }

    @Transient
    public PipelineStep getPipelineStep() {
        return pk.getPipelineStep();
    }

    public void setPipelineStep(PipelineStep pipelineStep) {
        pk.setPipelineStep(pipelineStep);
    }
    
    public void setPk(PipelineToPipelineStepsId pk) {
        this.pk = pk;
    }
    
    public PipelineToPipelineStepsId getPk() {
        return pk;
    }

    @Override
    @Transient
    public Long getPid() {
        return null;
    }

    @Override
    @Transient
    public void setPid(Long pid) {
    }
    
    @Override
    public int hashCode() {
        if (pk == null) {
            return 0;
        }
        return new HashCodeBuilder().append(pk.hashCode()).append(order.hashCode()).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PipelineToPipelineSteps)) {
            return false;
        }
        
        if (this == o) {
            return true;
        }
        
        PipelineToPipelineSteps p = (PipelineToPipelineSteps) o;
        
        return new EqualsBuilder() //
                .append(p.getOrder(), this.getOrder()) //
                .append(p.getPipeline().getName(), this.getPipeline().getName()) //
                .append(p.getPipelineStep().getName(), this.getPipelineStep().getName()) //
                .isEquals();
    }

}
