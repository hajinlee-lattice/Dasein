package com.latticeengines.domain.exposed.modelquality;

import java.io.IOException;
import java.util.Date;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.Transient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
/*
 * Table to save the template or predefined config
 */
@Entity
@Table(name = "MODELQUALITY_MODELCONFIG")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class ModelConfig implements HasPid, HasName, HasAuditingFields {

    private static final Logger log = LoggerFactory.getLogger(ModelConfig.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", nullable = false)
    private String name;

    @Column(name = "CONFIG_DATA", nullable = false)
    @Lob
    private byte[] configData;

    @Column(name = "CREATION_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date creationDate;

    @Column(name = "UPDATE_DATE", nullable = false)
    @Temporal(TemporalType.TIMESTAMP)
    private Date updateDate;

    @Column(name = "DESCRIPTION", length = 4000)
    private String description;

    @Transient
    private SelectedConfig selectedConfig;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Transient
    public SelectedConfig getSelectedConfig() {
        if (selectedConfig == null) {
            if (getConfigData() == null) {
                return null;
            }

            String uncompressedData = new String(CompressionUtils.decompressByteArray(getConfigData()));
            selectedConfig = JsonUtils.deserialize(uncompressedData, SelectedConfig.class);
        }
        return selectedConfig;
    }

    @Transient
    public void setSelectedConfig(SelectedConfig selectedConfig) {
        this.selectedConfig = selectedConfig;
        try {
            byte[] compressedData = CompressionUtils.compressByteArray(JsonUtils.serialize(selectedConfig).getBytes());
            setConfigData(compressedData);
        } catch (IOException e) {
            log.error("Failed to compress config", e);
        }
    }

    public byte[] getConfigData() {
        return configData;
    }

    public void setConfigData(byte[] configData) {
        this.configData = configData;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Date getCreated() {
        return creationDate;
    }

    @Override
    public void setCreated(Date date) {
        this.creationDate = date;
    }

    @Override
    public Date getUpdated() {
        return updateDate;
    }

    @Override
    public void setUpdated(Date date) {
        this.updateDate = date;
    }
}
