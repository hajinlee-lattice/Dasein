package com.latticeengines.domain.exposed.workflow;

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
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@Entity
@javax.persistence.Table(name = "SOURCE_FILE", uniqueConstraints = { @UniqueConstraint(columnNames = { "NAME",
        "TENANT_ID" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class SourceFile implements HasName, HasPid, HasTenant, HasTenantId, HasAuditingFields {

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION", nullable = true)
    private String description;

    @JsonProperty("tenant")
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("path")
    @Column(name = "PATH", nullable = false)
    private String path;

    @JsonProperty("errors_path")
    @Column(name = "ERRORS_PATH")
    private String errorsPath;

    @JsonProperty("created")
    @Column(name = "CREATED")
    private Date created;

    @JsonProperty("updated")
    @Column(name = "UPDATED")
    private Date updated;

    @JsonProperty("table_name")
    @Column(name = "TABLE_NAME")
    private String tableName;

    @JsonProperty("schema_interpretation")
    @Column(name = "SCHEMA_INTERPRETATION")
    @Enumerated(EnumType.STRING)
    private SchemaInterpretation schemaInterpretation;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getErrorsPath() {
        return errorsPath;
    }

    public void setErrorsPath(String errorsPath) {
        this.errorsPath = errorsPath;
    }

    public SchemaInterpretation getSchemaInterpretation() {
        return schemaInterpretation;
    }

    public void setSchemaInterpretation(SchemaInterpretation schemaInterpretation) {
        this.schemaInterpretation = schemaInterpretation;
    }
}
