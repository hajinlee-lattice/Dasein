package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.IsUserModifiable;

@Table(name = "MODEL_NOTES")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
public class ModelNotes implements HasPid, IsUserModifiable, Serializable {


    private static final long serialVersionUID = -863043454467222812L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("Id")
    @Column(name = "ID", unique = true, nullable = false)
    private String id;

    @JsonProperty("CreationTimestamp")
    @Column(name = "CREATION_TIMESTAMP")
    private Long creationTimestamp;

    @JsonProperty("LastModificationTimestamp")
    @Column(name = "LAST_MODIFICATION_TIMESTAMP")
    private Long lastModificationTimestamp;

    @JsonProperty("CreatedByUser")
    @Column(name = "CREATED_BY_USER")
    private String createdByUser;

    @JsonProperty("LastModifiedByUser")
    @Column(name = "LAST_MODIFIED_BY_USER")
    private String lastModifiedByUser;

    @ManyToOne
    @JoinColumn(name = "MODEL_ID", nullable = false)
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private ModelSummary modelSummary;

    @Column(name = "PARENT_MODEL_ID")
    @JsonProperty("ParentModelId")
    private String parentModelId;

    @JsonProperty("NotesContents")
    @Column(name = "NOTES_CONTENTS", length = 2048)
    private String notesContents;

    @JsonProperty("Origin")
    @Column(name = "ORIGIN")
    private String origin = "Note";

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(Long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public Long getLastModificationTimestamp() {
        return lastModificationTimestamp;
    }

    public void setLastModificationTimestamp(Long lastModificationTimestamp) {
        this.lastModificationTimestamp = lastModificationTimestamp;
    }

    public String getCreatedByUser() {
        return createdByUser;
    }

    public void setCreatedByUser(String createdByUser) {
        this.createdByUser = createdByUser;
    }

    @Override
    public String getLastModifiedByUser() {
        return lastModifiedByUser;
    }

    @Override
    public void setLastModifiedByUser(String lastModifiedByUser) {
        this.lastModifiedByUser = lastModifiedByUser;
    }

    public ModelSummary getModelSummary() {
        return modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public String getNotesContents() {
        return notesContents;
    }

    public void setNotesContents(String notesContents) {
        this.notesContents = notesContents;
    }

    public String getParentModelId() {
        return parentModelId;
    }

    public void setParentModelId(String parentModelId) {
        this.parentModelId = parentModelId;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

}
