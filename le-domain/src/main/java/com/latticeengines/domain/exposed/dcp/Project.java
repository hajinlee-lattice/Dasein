package com.latticeengines.domain.exposed.dcp;

import java.util.Date;
import java.util.List;

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
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasAuditingFields;
import com.latticeengines.domain.exposed.pls.SoftDeletable;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "DCP_PROJECT",
        indexes = { @Index(name = "IX_PROJECT_ID", columnList = "PROJECT_ID") },
        uniqueConstraints = {@UniqueConstraint(name = "UX_PROJECT_ID", columnNames = { "FK_TENANT_ID", "PROJECT_ID" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class Project implements HasPid, HasTenant, HasAuditingFields, SoftDeletable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonProperty("pid")
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne(cascade = {CascadeType.MERGE}, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonProperty("import_system")
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JoinColumn(name = "`FK_IMPORT_SYSTEM_ID`", nullable = false)
    private S3ImportSystem importSystem;

    @Column(name = "PROJECT_ID", nullable = false)
    @JsonProperty("project_id")
    private String projectId;

    @Column(name = "PROJECT_DISPLAY_NAME")
    @JsonProperty("project_display_name")
    private String projectDisplayName;

    @Enumerated(EnumType.STRING)
    @Column(name = "PROJECT_TYPE")
    @JsonProperty("project_type")
    private ProjectType projectType;

    @Column(name = "ROOT_PATH")
    @JsonProperty("root_path")
    private String rootPath;

    @Column(name = "CREATED", nullable = false)
    @JsonProperty("created")
    @Temporal(TemporalType.TIMESTAMP)
    private Date created;

    @Column(name = "UPDATED", nullable = false)
    @JsonProperty("updated")
    @Temporal(TemporalType.TIMESTAMP)
    private Date updated;

    @Column(name = "CREATED_BY", nullable = false)
    @JsonProperty("created_by")
    private String createdBy;

    @Column(name = "DELETED")
    @JsonProperty("deleted")
    private Boolean deleted;

    @Column(name = "PROJECT_DESCRIPTION", length = 4000)
    @JsonProperty("project_description")
    private String projectDescription;

    @Column(name = "RECIPIENT_LIST", columnDefinition = "'JSON'")
    @Type(type = "json")
    @JsonProperty("recipient_list")
    private List<String> recipientList;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public Tenant getTenant() {
        return tenant;
    }

    @Override
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public S3ImportSystem getS3ImportSystem() {
        return importSystem;
    }

    public void setS3ImportSystem(S3ImportSystem importSystem) {
        this.importSystem = importSystem;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectDisplayName() {
        return projectDisplayName;
    }

    public void setProjectDisplayName(String projectDisplayName) {
        this.projectDisplayName = projectDisplayName;
    }

    public ProjectType getProjectType() {
        return projectType;
    }

    public void setProjectType(ProjectType projectType) {
        this.projectType = projectType;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    @Override
    public Boolean getDeleted() {
        return deleted;
    }

    @Override
    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public String getProjectDescription() {
        return projectDescription;
    }

    public void setProjectDescription(String projectDescription) {
        this.projectDescription = projectDescription;
    }

    public List<String> getRecipientList() {
        return recipientList;
    }

    public void setRecipientList(List<String> recipientList) {
        this.recipientList = recipientList;
    }

    //TODO: define project type
    public enum ProjectType {
        Type1, Type2, Type3
    }
}
