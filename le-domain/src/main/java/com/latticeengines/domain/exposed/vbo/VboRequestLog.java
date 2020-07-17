package com.latticeengines.domain.exposed.vbo;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Entity
@Table(name = "VBO_REQUEST_LOG",
        indexes = {
                @Index(name = "IX_TENANT_ID", columnList = "TENANT_ID"),
                @Index(name = "IX_TRACE_ID", columnList = "TRACE_ID")},
        uniqueConstraints = {
                @UniqueConstraint(name = "UX_TRACE_ID", columnNames = {"TRACE_ID"})})
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class VboRequestLog implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "TRACE_ID")
    private String traceId;

    @Column(name = "TENANT_ID")
    private String tenantId;

    @Column(name = "VBO_REQUEST", columnDefinition = "'JSON'")
    @Type(type = "json")
    private VboRequest vboRequest;

    @Column(name = "VBO_RESPONSE", columnDefinition = "'JSON'")
    @Type(type = "json")
    private VboResponse vboResponse;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public VboRequest getVboRequest() {
        return vboRequest;
    }

    public void setVboRequest(VboRequest vboRequest) {
        this.vboRequest = vboRequest;
    }

    public VboResponse getVboResponse() {
        return vboResponse;
    }

    public void setVboResponse(VboResponse vboResponse) {
        this.vboResponse = vboResponse;
    }
}
