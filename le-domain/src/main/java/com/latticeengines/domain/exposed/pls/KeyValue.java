package com.latticeengines.domain.exposed.pls;

import java.io.IOException;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "KEY_VALUE")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class KeyValue implements HasTenantId, HasPid {
    
    private static final Log log = LogFactory.getLog(KeyValue.class);

    private Long pid;
    private Long tenantId;
    private byte[] data;
    private String ownerType = ModelSummary.class.getSimpleName();

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    @Index(name = "KEY_VALUE_TENANT_ID_IDX")
    public Long getTenantId() {
        return tenantId;
    }
    
    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Column(name = "DATA", nullable = false)
    @Lob
    @JsonIgnore
    public byte[] getData() {
        return data;
    }

    @JsonIgnore
    public void setData(byte[] data) {
        this.data = data;
    }
    
    @JsonProperty("Payload")
    @Transient
    public String getPayload() {
        byte[] uncompressedData = CompressionUtils.decompressByteArray(getData());
        JsonElement root = new JsonParser().parse(new String(uncompressedData));
        return root.toString();
    }
    
    @JsonProperty("Payload")
    @Transient
    public void setPayload(String payload) {
        byte[] payloadData = null;
        if (payload == null) {
            log.warn("Payload is null.");
            return;
        } else {
            payloadData = payload.getBytes();
        }
        
        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            setData(compressedData);
        } catch (IOException e) {
            log.error(e);
        }
    }

    @JsonIgnore
    @Column(name = "OWNER_TYPE", nullable = false)
    public String getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(String ownerType) {
        this.ownerType = ownerType;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
