package com.latticeengines.domain.exposed.modelquality;

import java.io.Serializable;

import javax.persistence.Embeddable;
import javax.persistence.ManyToOne;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

@Embeddable
public class PipelineToPipelineStepsId implements Serializable {

    private static final long serialVersionUID = -2530486729510934159L;

    @ManyToOne
    private Pipeline pipeline;
    
    @ManyToOne
    private PipelineStep pipelineStep;

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public PipelineStep getPipelineStep() {
        return pipelineStep;
    }

    public void setPipelineStep(PipelineStep pipelineStep) {
        this.pipelineStep = pipelineStep;
    }
    
    @Override
    public int hashCode() {
        if (pipeline == null) {
            return 0;
        }
        
        if (pipelineStep == null) {
            return 0;
        }
        return new HashCodeBuilder().append(pipeline.hashCode()).append(pipelineStep.hashCode()).toHashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PipelineToPipelineStepsId)) {
            return false;
        }
        
        if (this == o) {
            return true;
        }
        
        PipelineToPipelineStepsId p = (PipelineToPipelineStepsId) o;
        
        return new EqualsBuilder() //
                .append(p.getPipeline().getName(), this.getPipeline().getName()) //
                .append(p.getPipelineStep().getName(), this.getPipelineStep().getName()) //
                .isEquals();
    }
    
}
