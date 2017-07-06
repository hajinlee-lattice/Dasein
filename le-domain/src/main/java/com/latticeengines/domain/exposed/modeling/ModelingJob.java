package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PrimaryKeyJoinColumn;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.builder.EqualsBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Entity
@Table(name = "MODELING_JOB")
@PrimaryKeyJoinColumn(name = "JOB_PID")
public class ModelingJob extends Job {

    private Model model;
    private Long parentPid;
    private List<String> childIds = new ArrayList<String>();

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE })
    @JoinColumn(name = "FK_MODEL_ID")
    public Model getModel() {
        return model;
    }

    @JsonIgnore
    public void setModel(Model model) {
        this.model = model;
    }

    @Column(name = "PARENT_PID")
    public Long getParentPid() {
        return parentPid;
    }

    public void setParentPid(Long parentPid) {
        this.parentPid = parentPid;
    }

    @Column(name = "CHILD_JOB_IDS")
    public String getChildIds() {
        // convert list to csv string
        return StringTokenUtils.listToString(this.childIds);
    }

    public void setChildIds(String listOfIds) {
        // convert csv string to list
        this.childIds = StringTokenUtils.stringToList(listOfIds);
    }

    @Transient
    public List<String> getChildIdList() {
        return this.childIds;
    }

    public void addChildId(String jobId) {
        childIds.add(jobId);
    }

    public boolean equals(Object obj) {

        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }

        ModelingJob modelingJob = (ModelingJob) obj;

        return new EqualsBuilder().append(pid, modelingJob.getPid()).append(id, modelingJob.getId()).append(client, modelingJob.getClient()).append(appMasterProperties, modelingJob.getAppMasterPropertiesObject()).append(containerProperties, modelingJob.getContainerPropertiesObject()).append(model, modelingJob.getModel()).append(parentPid, modelingJob.getParentPid()).append(childIds, modelingJob.getChildIdList()).isEquals();
    }

}
