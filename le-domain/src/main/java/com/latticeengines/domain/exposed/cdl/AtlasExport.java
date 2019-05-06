package com.latticeengines.domain.exposed.cdl;

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
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.security.HasTenant;
import com.latticeengines.domain.exposed.security.HasTenantId;
import com.latticeengines.domain.exposed.security.Tenant;

@Entity
@Table(name = "ATLAS_EXPORT",
        indexes = { @Index(name = "IX_UUID", columnList = "UUID") },
        uniqueConstraints = {@UniqueConstraint(name = "UX_UUID", columnNames = { "TENANT_ID", "UUID" }) })
@Filter(name = "tenantFilter", condition = "FK_TENANT_ID = :tenantFilterId")
public class AtlasExport implements HasPid, HasTenant, HasTenantId {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "UUID", unique = true, nullable = false)
    @JsonProperty("uuid")
    private String uuid;

    @ManyToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "FK_TENANT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonIgnore
    @Column(name = "TENANT_ID", nullable = false)
    private Long tenantId;

    @JsonProperty("export_type")
    @Column(name = "EXPORT_TYPE")
    @Enumerated(EnumType.STRING)
    private AtlasExportType exportType;

//    @JsonProperty("system_path")
//    @Column(name = "SYSTEM_PATH")
//    private String systemPath;
//
//    @JsonProperty("dropfolder_path")
//    @Column(name = "DROPFOLDER_PATH")
//    private String dropfolderPath;

    @JsonProperty("date_prefix")
    @Column(name = "DATE_PREFIX", length = 15)
    private String datePrefix;

    @JsonProperty("files_under_system_path")
    @Column(name = "FILES_UNDER_SYSTEM_PATH", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> filesUnderSystemPath;

    @JsonProperty("files_under_dropfolder")
    @Column(name = "FILES_UNDER_DROPFOLDER", columnDefinition = "'JSON'")
    @Type(type = "json")
    private List<String> filesUnderDropFolder;

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
        if (tenant != null) {
            setTenantId(tenant.getPid());
        }
    }

    @Override
    public Long getTenantId() {
        return tenantId;
    }

    @Override
    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public AtlasExportType getExportType() {
        return exportType;
    }

    public void setExportType(AtlasExportType exportType) {
        this.exportType = exportType;
    }

    public String getDatePrefix() {
        return datePrefix;
    }

    public void setDatePrefix(String datePrefix) {
        this.datePrefix = datePrefix;
    }

    public List<String> getFilesUnderSystemPath() {
        return filesUnderSystemPath;
    }

    public void setFilesUnderSystemPath(List<String> filesUnderSystemPath) {
        this.filesUnderSystemPath = filesUnderSystemPath;
    }

    public List<String> getFilesUnderDropFolder() {
        return filesUnderDropFolder;
    }

    public void setFilesUnderDropFolder(List<String> filesUnderDropFolder) {
        this.filesUnderDropFolder = filesUnderDropFolder;
    }

    //    public String getSystemPath() {
//        return systemPath;
//    }
//
//    public void setSystemPath(String systemPath) {
//        this.systemPath = systemPath;
//    }
//
//    public String getDropfolderPath() {
//        return dropfolderPath;
//    }
//
//    public void setDropfolderPath(String dropfolderPath) {
//        this.dropfolderPath = dropfolderPath;
//    }
}
