package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;

@Entity
@Table(name = "TALKINGPOINT")
@JsonIgnoreProperties(ignoreUnknown = true)
public class TalkingPoint implements HasPid, HasName, HasAuditingFields {
    public static final String TALKING_POINT_NAME_PREFIX = "TP";
    public static final String TALKING_POINT_NAME_FORMAT = "%s__%s";

    public TalkingPoint() {
        setName(generateNameStr());
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("play_id")
    @Column(name = "PLAY_ID", nullable = false)
    private Long playId;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "PLAY_ID", updatable = false, insertable = false)
    private Play play;

    @JsonProperty("title")
    @Column(name = "TITLE", nullable = false)
    private String title;

    @JsonProperty("content")
    @Column(name = "CONTENT", nullable = false)
    @Type(type = "text")
    private String content;

    @JsonProperty("offset")
    @Column(name = "OFFSET", nullable = false)
    private int offset;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    public Long getPid() {
        return pid;
    }

    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getPlayId() {
        return playId;
    }

    public void setPlayId(Long playId) {
        this.playId = playId;
    }

    public Play getPlay() {
        return play;
    }

    public void setPlay(Play play) {
        this.play = play;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(Date updated) {
        this.updated = updated;
    }

    public String generateNameStr() {
        return String.format(TALKING_POINT_NAME_PREFIX, TALKING_POINT_NAME_FORMAT, UUID.randomUUID().toString());
    }
}
