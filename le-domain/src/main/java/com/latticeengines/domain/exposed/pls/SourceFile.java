package com.latticeengines.domain.exposed.pls;

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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@Entity
@javax.persistence.Table(name = "SOURCE_FILE", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "NAME", "TENANT_ID" }) })
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
public class SourceFile
        implements HasName, HasPid, HasTenant, HasTenantId, HasAuditingFields, HasApplicationId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    private String name;

    @JsonProperty("description")
    @Column(name = "DESCRIPTION")
    private String description;

    @JsonProperty("display_name")
    @Column(name = "DISPLAY_NAME")
    private String displayName;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("path")
    @Column(name = "PATH", nullable = false, length = 2048)
    private String path;

    @JsonProperty("file_rows")
    @Column(name = "FILE_ROWS", nullable = true)
    private Long fileRows;

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

    @JsonProperty("application_id")
    @Column(name = "APPLICATION_ID")
    private String applicationId;

    @JsonProperty("state")
    @Column(name = "STATE")
    @Enumerated(EnumType.STRING)
    private SourceFileState state;

    @JsonProperty("entity")
    @Column(name = "BUSINESS_ENTITY")
    @Enumerated(EnumType.STRING)
    private BusinessEntity businessEntity;

    @JsonProperty("partial_file")
    @Column(name = "PARTIAL_FILE")
    private boolean partialFile;

    @JsonProperty("s3_path")
    @Column(name = "S3_PATH")
    private String s3Path;

    @JsonProperty("s3_bucket")
    @Column(name = "S3_BUCKET")
    private String s3Bucket;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Long getFileRows() {
        return fileRows;
    }

    public void setFileRows(Long fileRows) {
        this.fileRows = fileRows;
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

    public SchemaInterpretation getSchemaInterpretation() {
        return schemaInterpretation;
    }

    public void setSchemaInterpretation(SchemaInterpretation schemaInterpretation) {
        this.schemaInterpretation = schemaInterpretation;
    }

    @Override
    public String getApplicationId() {
        return applicationId;
    }

    @Override
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public SourceFileState getState() {
        return state;
    }

    public void setState(SourceFileState state) {
        this.state = state;
    }

    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }

    public void setBusinessEntity(BusinessEntity businessEntity) {
        this.businessEntity = businessEntity;
    }

    public boolean isPartialFile() {
        return partialFile;
    }

    public void setPartialFile(boolean partialFile) {
        this.partialFile = partialFile;
    }

    public String getS3Path() {
        return s3Path;
    }

    public void setS3Path(String s3Path) {
        this.s3Path = s3Path;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }
}
