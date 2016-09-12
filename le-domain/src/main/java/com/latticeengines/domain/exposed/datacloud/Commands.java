package com.latticeengines.domain.exposed.datacloud;

import java.util.Date;
import java.util.List;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "Commands")
public class Commands implements HasPid {
    @Id
    @Column(name = "CommandId", unique = true, nullable = false)
    private Long commandId;

    @OneToOne(fetch = FetchType.LAZY)
    @PrimaryKeyJoinColumn
    private CommandIds commandIdsEntity;

    @OneToMany(cascade = { CascadeType.MERGE }, fetch = FetchType.LAZY, mappedBy = "command")
    @OnDelete(action = OnDeleteAction.CASCADE)
    @Fetch(FetchMode.SUBSELECT)
    private List<CommandParameter> commandParameters;

    @Column(name = "CommandName", nullable = false)
    private String commandName;

    @Column(name = "SourceTable", nullable = false)
    private String sourceTable;

    @Column(name = "DestTables", nullable = false)
    private String destTables;

    @Column(name = "CommandStatus", nullable = false)
    private Integer commandStatus;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name = "CreateTime", nullable = false)
    private Date createTime;

    @Column(name = "Contract_External_ID", nullable = false)
    private String contractExternalID;

    @Column(name = "Deployment_External_ID", nullable = false)
    private String deploymentExternalID;

    @Column(name = "Process_UID", nullable = false)
    private String processUID;

    @Column(name = "IsDownloading", nullable = false)
    private Boolean isDownloading;

    @Column(name = "Num_Retries", nullable = false)
    private Integer numRetries;

    @Column(name = "Max_Num_Retries", nullable = false)
    private Integer maxNumRetries;

    @Column(name = "ProfileID", nullable = true)
    private String profileID;

    public Commands() {
        super();
    }

    @Override
    @JsonProperty("CommandID")
    public Long getPid() {
        return this.commandId;
    }

    @Override
    @JsonProperty("CommandID")
    public void setPid(Long id) {
        this.commandId = id;
    }

    @JsonIgnore
    public CommandIds getCommandIdsEntity() {
        return commandIdsEntity;
    }

    @JsonIgnore
    public void setCommandIdsEntity(CommandIds commandIdsEntity) {
        this.commandIdsEntity = commandIdsEntity;
    }

    @JsonProperty("CommandName")
    public String getCommandName() {
        return commandName;
    }

    @JsonProperty("CommandName")
    public void setCommandName(String commandName) {
        this.commandName = commandName;
    }

    @JsonProperty("SourceTable")
    public String getSourceTable() {
        return sourceTable;
    }

    @JsonProperty("SourceTable")
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    @JsonProperty("DestTables")
    public String getDestTables() {
        return destTables;
    }

    @JsonProperty("DestTables")
    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

    @JsonProperty("CommandStatus")
    public Integer getCommandStatus() {
        return commandStatus;
    }

    @JsonProperty("CommandStatus")
    public void setCommandStatus(Integer commandStatus) {
        this.commandStatus = commandStatus;
    }

    @JsonProperty("CreateTime")
    public Date getCreateTime() {
        return createTime;
    }

    @JsonProperty("CreateTime")
    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @JsonProperty("ContractExternalID")
    public String getContractExternalID() {
        return contractExternalID;
    }

    @JsonProperty("ContractExternalID")
    public void setContractExternalID(String contractExternalID) {
        this.contractExternalID = contractExternalID;
    }

    @JsonProperty("DeploymentExternalID")
    public String getDeploymentExternalID() {
        return deploymentExternalID;
    }

    @JsonProperty("DeploymentExternalID")
    public void setDeploymentExternalID(String deploymentExternalID) {
        this.deploymentExternalID = deploymentExternalID;
    }

    @JsonProperty("ProcessUID")
    public String getProcessUID() {
        return processUID;
    }

    @JsonProperty("ProcessUID")
    public void setProcessUID(String processUID) {
        this.processUID = processUID;
    }

    @JsonProperty("IsDownloading")
    public Boolean getIsDownloading() {
        return isDownloading;
    }

    @JsonProperty("IsDownloading")
    public void setIsDownloading(Boolean isDownloading) {
        this.isDownloading = isDownloading;
    }

    @JsonProperty("NumRetries")
    public Integer getNumRetries() {
        return numRetries;
    }

    @JsonProperty("NumRetries")
    public void setNumRetries(Integer numRetries) {
        this.numRetries = numRetries;
    }

    @JsonProperty("MaxNumRetries")
    public Integer getMaxNumRetries() {
        return maxNumRetries;
    }

    @JsonProperty("MaxNumRetries")
    public void setMaxNumRetries(Integer maxNumRetries) {
        this.maxNumRetries = maxNumRetries;
    }

    @JsonProperty("ProfileID")
    public String getProfileID() {
        return profileID;
    }

    @JsonProperty("ProfileID")
    public void setProfileID(String profileID) {
        this.profileID = profileID;
    }

    @Override
    public int hashCode() {
        return getPid().hashCode();
    }

    @Override
    public boolean equals(Object other) {

        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (!other.getClass().equals(this.getClass())) {
            return false;
        }
        CommandIds theOther = (CommandIds) other;

        return new EqualsBuilder().append(commandId, theOther.getPid())
                .isEquals();

    }

    @Override
    public String toString() {
        return "Command [commandId=" + commandId + ", commandName="
                + commandName + ", sourceTable=" + sourceTable
                + ", destTables=" + destTables + ", commandStatus="
                + commandStatus + ", createTime=" + createTime
                + ", contractExternalID=" + contractExternalID
                + ", deploymentExternalID=" + deploymentExternalID
                + ", processUID=" + processUID + ", isDownloading="
                + isDownloading + ", numRetries=" + numRetries
                + ", maxNumRetries=" + maxNumRetries + ", profileID="
                + profileID + "]";
    }

}
