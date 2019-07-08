package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Entity
@Table(name = "EXPORT_FIELD_METADATA_DEFAULTS", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "ATTR_NAME", "EXT_SYS_NAME" }) })
public class ExportFieldMetadataDefaults implements HasPid {

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("attrName")
    @Column(name = "ATTR_NAME", nullable = false)
    private String attrName;

    @JsonProperty("displayName")
    @Column(name = "DISPLAY_NAME", nullable = false)
    private String displayName;

    @JsonProperty("entity")
    @Enumerated(EnumType.STRING)
    @Column(name = "ENTITY", nullable = false)
    private BusinessEntity entity;

    @JsonProperty("javaClass")
    @Column(name = "JAVA_CLASS", nullable = false)
    private String javaClass;

    @JsonProperty(CDLConstants.EXTERNAL_SYSTEM_NAME)
    @Column(name = "EXT_SYS_NAME", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLExternalSystemName externalSystemName;

    @JsonProperty("standardField")
    @Column(name = "STANDARD_FIELD", nullable = false)
    private Boolean standardField;

    @JsonProperty("exportEnabled")
    @Column(name = "EXPORT_ENABLED", nullable = false)
    private Boolean exportEnabled;

    @JsonProperty("historyEnabled")
    @Column(name = "HISTORY_ENABLED", nullable = false)
    private Boolean historyEnabled;

    public ExportFieldMetadataDefaults() {

    }

    public ExportFieldMetadataDefaults(String attrName, String displayName, String javaClass, BusinessEntity entity,
            CDLExternalSystemName systemName,
            Boolean isStandardField,
            Boolean isExportEnabled, Boolean isHistoryEnabled) {
        this.attrName = attrName;
        this.entity = entity;
        this.displayName = displayName;
        this.javaClass = javaClass;
        this.externalSystemName = systemName;
        this.standardField = isStandardField;
        this.exportEnabled = isExportEnabled;
        this.historyEnabled = isHistoryEnabled;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getAttrName() {
        return attrName;
    }

    public void setAttrName(String attrName) {
        this.attrName = attrName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    public CDLExternalSystemName getExternalSystemName() {
        return externalSystemName;
    }

    public void setExternalSystemName(CDLExternalSystemName externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public Boolean getStandardField() {
        return standardField;
    }

    public void setStandardField(Boolean standardField) {
        this.standardField = standardField;
    }

    public Boolean getExportEnabled() {
        return exportEnabled;
    }

    public void setExportEnabled(Boolean exportEnabled) {
        this.exportEnabled = exportEnabled;
    }

    public Boolean getHistoryEnabled() {
        return historyEnabled;
    }

    public void setHistoryEnabled(Boolean historyEnabled) {
        this.historyEnabled = historyEnabled;
    }

}
