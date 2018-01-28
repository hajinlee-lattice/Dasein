package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.Map;

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
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.Transient;
import javax.persistence.UniqueConstraint;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.Filters;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@javax.persistence.Table(name = "METADATA_STATISTICS", //
uniqueConstraints = { @UniqueConstraint(columnNames = { "TENANT_ID", "NAME" }) })
@Filters({ //
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId") })
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatisticsContainer implements HasPid, HasName, HasTenantId, HasTenant {
    private static final Logger log = LoggerFactory.getLogger(StatisticsContainer.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "NAME", unique = true, nullable = false)
    @JsonProperty("Name")
    private String name;

    //TODO: to remove after M18
    @Column(name = "DATA", nullable = true)
    @Lob
    @JsonIgnore
    private byte[] data;

    //TODO: change nullable to false after M18
    @Column(name = "CUBES_DATA", nullable = true)
    @Lob
    @JsonIgnore
    private byte[] cubesData;

    @ManyToOne(cascade = CascadeType.MERGE, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment segment;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("version")
    @Enumerated(EnumType.STRING)
    @Column(name = "VERSION", nullable = false)
    private DataCollection.Version version;

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Deprecated
    @JsonProperty("Statistics")
    @Transient
    public Statistics getStatistics() {
        if (getData() == null) {
            return null;
        }

        String uncompressedData = new String(CompressionUtils.decompressByteArray(getData()));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            return JsonUtils.deserialize(uncompressedData, Statistics.class);
        } else {
            return null;
        }
    }

    @Deprecated
    @JsonProperty("Statistics")
    @Transient
    public void setStatistics(Statistics statistics) {
        if (statistics == null) {
            setData(null);
            return;
        }
        String string = JsonUtils.serialize(statistics);
        byte[] payloadData = string.getBytes();

        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            setData(compressedData);
        } catch (IOException e) {
            log.error("Failed to compress payload [" + statistics + "]", e);
        }
    }

    private byte[] getCubesData() {
        return cubesData;
    }

    private void setCubesData(byte[] cubesData) {
        this.cubesData = cubesData;
    }

    @JsonProperty("StatsCubes")
    @Transient
    public Map<String, StatsCube> getStatsCubes() {
        if (getCubesData() == null) {
            return null;
        }

        String uncompressedData = new String(CompressionUtils.decompressByteArray(getCubesData()));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            return JsonUtils.deserialize(uncompressedData, new TypeReference<Map<String, StatsCube>>() {});
        } else {
            return null;
        }
    }

    @JsonProperty("StatsCubes")
    @Transient
    public void setStatsCubes(Map<String, StatsCube> cubes) {
        if (MapUtils.isEmpty(cubes)) {
            setCubesData(null);
            return;
        }
        String string = JsonUtils.serialize(cubes);
        byte[] payloadData = string.getBytes();
        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            setCubesData(compressedData);
        } catch (IOException e) {
            log.error("Failed to compress payload [" + cubes + "]", e);
        }
    }

    public static Logger getLog() {
        return log;
    }

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

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
        this.tenant = tenant;
    }

    public MetadataSegment getSegment() {
        return segment;
    }

    public void setSegment(MetadataSegment segment) {
        this.segment = segment;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }
}
