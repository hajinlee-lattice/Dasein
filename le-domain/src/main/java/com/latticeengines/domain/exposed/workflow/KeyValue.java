package com.latticeengines.domain.exposed.workflow;

import java.io.IOException;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.HasTenantId;

@Entity
@Table(name = "KEY_VALUE", //
        indexes = { @Index(name = "KEY_VALUE_TENANT_ID_IDX", columnList = "TENANT_ID") })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
public class KeyValue implements HasTenantId, HasPid {

    private static final Logger log = LoggerFactory.getLogger(KeyValue.class);

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
        if (getData() == null) {
            return null;
        }

        String uncompressedData = new String(CompressionUtils.decompressByteArray(getData()));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            JsonNode root = JsonUtils.deserialize(uncompressedData, JsonNode.class);
            return root.toString();
        } else {
            return null;
        }
    }

    @JsonProperty("Payload")
    @Transient
    public void setPayload(String payload) {
        byte[] payloadData = null;
        if (StringUtils.isBlank(payload)) {
            log.warn("Payload is null.");
            return;
        } else {
            payloadData = payload.getBytes();
        }

        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            setData(compressedData);
        } catch (IOException e) {
            log.error("Failed to compress payload [" + payload + "]", e);
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
