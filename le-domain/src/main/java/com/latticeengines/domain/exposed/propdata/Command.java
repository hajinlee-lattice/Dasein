package com.latticeengines.domain.exposed.propdata;

import java.util.Date;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.apache.commons.lang.builder.EqualsBuilder;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "Commands")
public class Command implements HasPid {

    @Id
    @Column(name = "CommandId", unique = true, nullable = false)
    private Long commandId;

    @OneToOne(fetch = FetchType.LAZY)
    @PrimaryKeyJoinColumn
    private CommandId commandIds;

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

    public Command() {
        super();
    }

    @Override
    public Long getPid() {
        return this.commandId;
    }

    @Override
    public void setPid(Long id) {
        this.commandId = id;
    }

    public CommandId getCommandIds() {
        return commandIds;
    }

    public void setCommandIds(CommandId commandIds) {
        this.commandIds = commandIds;
    }

    public String getCommandName() {
        return commandName;
    }

    public void setCommandName(String commandName) {
        this.commandName = commandName;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getDestTables() {
        return destTables;
    }

    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

    public Integer getCommandStatus() {
        return commandStatus;
    }

    public void setCommandStatus(Integer commandStatus) {
        this.commandStatus = commandStatus;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getContractExternalID() {
        return contractExternalID;
    }

    public void setContractExternalID(String contractExternalID) {
        this.contractExternalID = contractExternalID;
    }

    public String getDeploymentExternalID() {
        return deploymentExternalID;
    }

    public void setDeploymentExternalID(String deploymentExternalID) {
        this.deploymentExternalID = deploymentExternalID;
    }

    public String getProcessUID() {
        return processUID;
    }

    public void setProcessUID(String processUID) {
        this.processUID = processUID;
    }

    public Boolean getIsDownloading() {
        return isDownloading;
    }

    public void setIsDownloading(Boolean isDownloading) {
        this.isDownloading = isDownloading;
    }

    public Integer getNumRetries() {
        return numRetries;
    }

    public void setNumRetries(Integer numRetries) {
        this.numRetries = numRetries;
    }

    public Integer getMaxNumRetries() {
        return maxNumRetries;
    }

    public void setMaxNumRetries(Integer maxNumRetries) {
        this.maxNumRetries = maxNumRetries;
    }

    public String getProfileID() {
        return profileID;
    }

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
        CommandId theOther = (CommandId) other;

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
