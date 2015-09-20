package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.OrderBy;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;
import org.hibernate.annotations.Parameter;
import org.hibernate.annotations.Type;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "LeadScoringCommand")
public class ModelCommand implements HasPid, Serializable {

    public static final String TAHOE = "Tahoe";
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "CommandId", nullable = false)
    private Long commandId;

    @Column(name = "Deployment_External_ID", nullable = false)
    private String deploymentExternalId;

    @Column(name = "Contract_External_ID", nullable = false)
    private String contractExternalId;

    @Column(name = "LeadInputTableName", nullable = false)
    private String eventTable;

    @Column(name = "CommandStatus", nullable = false)
    @Type(type = "com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatusUserType", parameters = {
            @Parameter(name = "enumClassName", value = "com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus"),
            @Parameter(name = "identifierMethod", value = "getValue"),
            @Parameter(name = "valueOfMethod", value = "valueOf") })
    private ModelCommandStatus commandStatus;

    @Immutable
    @OneToMany(mappedBy = "modelCommand", fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @LazyCollection(LazyCollectionOption.FALSE)
    private List<ModelCommandParameter> commandParameters;

    @Column(name = "ModelCommandStep")
    @Enumerated(EnumType.STRING)
    private ModelCommandStep modelCommandStep;

    @Immutable
    @OneToMany(mappedBy = "modelCommand", fetch = FetchType.LAZY)
    @LazyCollection(LazyCollectionOption.TRUE)
    @OrderBy("Created ASC")
    private List<ModelCommandLog> commandLogs;

    @Immutable
    @OneToMany(mappedBy = "modelCommand", fetch = FetchType.LAZY)
    @LazyCollection(LazyCollectionOption.TRUE)
    @OrderBy("Created ASC")
    private List<ModelCommandState> commandStates;

    @Column(name = "ModelId", nullable = false)
    private String modelId;

    @OneToOne(mappedBy = "modelCommand", fetch = FetchType.LAZY)
    private ModelCommandResult modelCommandResult;

    ModelCommand() {
        super();
    }

    @VisibleForTesting
    public ModelCommand(Long commandId, String contractExternalId, String deploymentExternalId,
            ModelCommandStatus commandStatus, List<ModelCommandParameter> commandParameters, String modelId, String eventTable) {
        super();
        this.commandId = commandId;
        this.deploymentExternalId = deploymentExternalId;
        this.contractExternalId = contractExternalId;
        this.commandStatus = commandStatus;
        this.commandParameters = commandParameters;
        this.modelId = modelId;
        this.eventTable = eventTable;
    }

    @Override
    public Long getPid() {
        return this.commandId;
    }

    @Override
    public void setPid(Long id) {
        this.commandId = id;
    }

    public String getDeploymentExternalId() {
        return deploymentExternalId;
    }

    public String getContractExternalId() {
        return contractExternalId;
    }

    public String getEventTable() {
        return eventTable;
    }

    public ModelCommandStatus getCommandStatus() {
        return commandStatus;
    }

    public void setCommandStatus(ModelCommandStatus commandStatus) {
        this.commandStatus = commandStatus;
    }

    public List<ModelCommandParameter> getCommandParameters() {
        return Collections.unmodifiableList(commandParameters);
    }

    @Transient
    public boolean isNew() {
        return commandStatus.equals(ModelCommandStatus.NEW);
    }

    @Transient
    public boolean isInProgress() {
        return commandStatus.equals(ModelCommandStatus.IN_PROGRESS);
    }

    public ModelCommandStep getModelCommandStep() {
        return modelCommandStep;
    }

    public void setModelCommandStep(ModelCommandStep modelCommandStep) {
        this.modelCommandStep = modelCommandStep;
    }

    public List<ModelCommandLog> getCommandLogs() {
        return Collections.unmodifiableList(commandLogs);
    }

    public List<ModelCommandState> getCommandStates() {
        return commandStates;
    }

    public String getModelId() {
        return modelId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((commandId == null) ? 0 : commandId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ModelCommand other = (ModelCommand) obj;
        if (commandId == null) {
            if (other.commandId != null)
                return false;
        } else if (!commandId.equals(other.commandId))
            return false;
        return true;
    }

}
