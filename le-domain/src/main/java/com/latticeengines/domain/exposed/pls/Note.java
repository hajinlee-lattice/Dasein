package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.IsUserModifiable;

@MappedSuperclass
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = ModelNote.class, name = "modelNote"), //
        @Type(value = RatingEngineNote.class, name = "ratingEngineNote") })
public abstract class Note implements HasId<String>, HasPid, IsUserModifiable, Serializable {

    private static final long serialVersionUID = 1L;

    public Note() {
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", nullable = false)
    private Long pid;

    @JsonProperty("Id")
    @Column(name = "ID", unique = true, nullable = false)
    private String id;

    @JsonProperty("CreationTimestamp")
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    private Long creationTimestamp;

    @JsonProperty("LastModificationTimestamp")
    @Column(name = "LAST_MODIFICATION_TIMESTAMP", nullable = false)
    private Long lastModificationTimestamp;

    @JsonProperty("CreatedByUser")
    @Column(name = "CREATED_BY_USER")
    private String createdByUser;

    @JsonProperty("LastModifiedByUser")
    @Column(name = "LAST_MODIFIED_BY_USER")
    private String lastModifiedByUser;

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

    @Override
    public String getId() {
        return id;
    }

    @Override
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

    public String getNotesContents() {
        return notesContents;
    }

    public void setNotesContents(String notesContents) {
        this.notesContents = notesContents;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
