package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

@Entity
@Table(name = "ATLAS_S3_IMPORT_MESSAGE", uniqueConstraints = {
        @UniqueConstraint(name = "UX_KEY", columnNames = { "KEY" }) })
public class S3ImportMessage implements HasPid, HasAuditingFields {

    @JsonProperty("pid")
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    /*
     * This column is used to drop all messages if a dropbox has been delete.
     */
    @JsonProperty("dropbox")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_DROP_BOX")
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DropBox dropBox;

    @JsonProperty("bucket")
    @Column(name = "BUCKET")
    private String bucket;

    @JsonProperty("key")
    @Column(name = "KEY", length = 500, nullable = false)
    private String key;

    @JsonProperty("feed_type")
    @Column(name = "FEED_TYPE")
    private String feedType;

    @JsonProperty("host_url")
    @Column(name = "HOST_URL")
    private String hostUrl;

    @JsonProperty("message_type")
    @Column(name = "MESSAGE_TYPE", length = 25)
    @Enumerated(EnumType.STRING)
    private S3ImportMessageType messageType;

    @JsonProperty("created")
    @Column(name = "CREATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DropBox getDropBox() {
        return dropBox;
    }

    public void setDropBox(DropBox dropBox) {
        this.dropBox = dropBox;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        if (StringUtils.isEmpty(key) || key.split("/").length < 4) {
            throw new IllegalArgumentException("Do not support s3 message key: " + key);
        }
        this.key = key;
    }

    public String getFeedType() {
        return feedType;
    }

    public void setFeedType(String feedType) {
        this.feedType = feedType;
    }

    public String getHostUrl() {
        return hostUrl;
    }

    public void setHostUrl(String hostUrl) {
        this.hostUrl = hostUrl;
    }

    public S3ImportMessageType getMessageType() {
        return messageType;
    }

    public void setMessageType(S3ImportMessageType messageType) {
        this.messageType = messageType;
    }

    @Override
    public Date getCreated() {
        return created;
    }

    @Override
    public void setCreated(Date created) {
        this.created = created;
    }

    @Override
    public Date getUpdated() {
        return updated;
    }

    @Override
    public void setUpdated(Date updated) {
        this.updated = updated;
    }
}
