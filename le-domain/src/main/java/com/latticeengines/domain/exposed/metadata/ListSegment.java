package com.latticeengines.domain.exposed.metadata;

import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;

import org.apache.commons.collections4.MapUtils;
import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;

@Entity
@javax.persistence.Table(name = "METADATA_LIST_SEGMENT")
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ListSegment implements HasPid {

    private static final Logger log = LoggerFactory.getLogger(ListSegment.class);

    public static final String RAW_IMPORT = "RawImport";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("externalSystem")
    @Column(name = "EXTERNAL_SYSTEM")
    private String externalSystem;

    @JsonProperty("externalSegmentId")
    @Column(name = "EXTERNAL_SEGMENT_ID", nullable = false)
    private String externalSegmentId;

    @JsonProperty("s3DropFolder")
    @Column(name = "S3_DROP_FOLDER", nullable = false)
    private String s3DropFolder;

    //  Map<String, String> --> <entity --> template id>
    @JsonProperty("dataTemplates")
    @Column(name = "DATA_TEMPLATES", columnDefinition = "'JSON'")
    @Type(type = "json")
    private Map<String, String> dataTemplates;

    @JsonProperty("csvAdaptor")
    @Column(name = "CSV_ADAPTOR", columnDefinition = "'JSON'")
    @Type(type = "json")
    private CSVAdaptor csvAdaptor;

    @Column(name = "TENANT_ID", nullable = false)
    @JsonIgnore
    private Long tenantId;

    @JsonIgnore
    @OneToOne(fetch = FetchType.LAZY, cascade = {CascadeType.MERGE})
    @JoinColumn(name = "FK_SEGMENT_ID", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private MetadataSegment metadataSegment;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public String getExternalSystem() {
        return externalSystem;
    }

    public void setExternalSystem(String externalSystem) {
        this.externalSystem = externalSystem;
    }

    public String getExternalSegmentId() {
        return externalSegmentId;
    }

    public void setExternalSegmentId(String externalSegmentId) {
        this.externalSegmentId = externalSegmentId;
    }

    public String getS3DropFolder() {
        return s3DropFolder;
    }

    public void setS3DropFolder(String s3DropFolder) {
        this.s3DropFolder = s3DropFolder;
    }

    public CSVAdaptor getCsvAdaptor() {
        return csvAdaptor;
    }

    public void setCsvAdaptor(CSVAdaptor csvAdaptor) {
        this.csvAdaptor = csvAdaptor;
    }

    public Map<String, String> getDataTemplates() {
        return dataTemplates;
    }

    public void setDataTemplates(Map<String, String> dataTemplates) {
        this.dataTemplates = dataTemplates;
    }

    public MetadataSegment getMetadataSegment() {
        return metadataSegment;
    }

    public void setMetadataSegment(MetadataSegment metadataSegment) {
        this.metadataSegment = metadataSegment;
    }

    public Long getTenantId() {
        return tenantId;
    }

    public void setTenantId(Long tenantId) {
        this.tenantId = tenantId;
    }

    public String getTemplateId(String templateKey) {
        if (MapUtils.isNotEmpty(dataTemplates)) {
            return dataTemplates.get(templateKey);
        } else {
            return null;
        }
    }
}
