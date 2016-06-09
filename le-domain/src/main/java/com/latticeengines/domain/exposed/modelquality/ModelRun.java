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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "MODELQUALITY_MODELRUN")
@JsonIgnoreProperties({"hibernateLazyInitializer", "handler"})
public class ModelRun implements HasPid {
    
    private static final Log log = LogFactory.getLog(ModelRun.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @Column(name = "CONFIG_DATA", nullable = false)
    @Lob
    private byte[] configData;
    
    @Temporal(TemporalType.TIME)
    private Date creationDate;
    
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

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    
}
