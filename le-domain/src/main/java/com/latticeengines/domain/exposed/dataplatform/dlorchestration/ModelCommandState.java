package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.jpa.AbstractTimestampEntity;

@Entity
@Access(AccessType.FIELD)
@Table(name = "LeadScoringCommandState")
public class ModelCommandState extends AbstractTimestampEntity implements HasPid, Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne()
    @JoinColumn(name = "CommandId", nullable = false)
    private ModelCommand modelCommand;

    @Column(name = "ModelCommandStep", nullable = false)
    @Enumerated(EnumType.STRING)
    private ModelCommandStep modelCommandStep;

    @Column(name = "YarnApplicationId")
    private String yarnApplicationId;

    @Column(name = "Status")
    @Enumerated(EnumType.STRING)
    private FinalApplicationStatus status;

    @Column(name = "Progress")
    private Float progress;

    @Column(name = "Diagnostics", length = 65535)
    private String diagnostics;

    @Column(name = "TrackingUrl", length = 65535)
    private String trackingUrl;

    @Column(name = "ElapsedTimeInMillis")
    private Long elapsedTimeInMillis;

    ModelCommandState() {
        super();
    }

    public ModelCommandState(ModelCommand modelCommand, ModelCommandStep modelCommandStep) {
        super();
        this.modelCommand = modelCommand;
        this.modelCommandStep = modelCommandStep;
    }

    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long id) {
        this.pid = id;
    }

    public ModelCommand getModelCommand() {
        return modelCommand;
    }

    public ModelCommandStep getModelCommandStep() {
        return modelCommandStep;
    }

    public String getYarnApplicationId() {
        return yarnApplicationId;
    }

    public void setYarnApplicationId(String yarnApplicationId) {
        this.yarnApplicationId = yarnApplicationId;
    }

    public FinalApplicationStatus getStatus() {
        return status;
    }

    public void setStatus(FinalApplicationStatus status) {
        this.status = status;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    public String getDiagnostics() {
        return diagnostics;
    }

    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    public Long getElapsedTimeInMillis() {
        return elapsedTimeInMillis;
    }

    public void setElapsedTimeInMillis(Long elapsedTimeInMillis) {
        this.elapsedTimeInMillis = elapsedTimeInMillis;
    }

}
