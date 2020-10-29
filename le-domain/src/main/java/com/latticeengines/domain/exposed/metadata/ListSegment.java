package com.latticeengines.domain.exposed.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Transient;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;

@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ListSegment implements HasPid {

    private static final Logger log = LoggerFactory.getLogger(ListSegment.class);

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
    @Column(name = "EXTERNAL_SYSTEM_ID")
    private String externalSegmentId;

    @JsonProperty("s3DropFolder")
    @Column(name = "S3_DROP_FOLDER")
    private String s3DropFolder;

    @Column(name = "DATA_TEMPLATES")
    @Lob
    @JsonIgnore
    private String dataTemplates;

    @Column(name = "CSV_ADAPTOR")
    @Lob
    @JsonIgnore
    private byte[] csvAdaptor;

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

    //  Map<String, String> --> <entity --> template id>
    @JsonProperty("dataTemplateMap")
    @Transient
    public Map<String, String> getDataTemplateMap() {
        String data = getDataTemplates();
        if (StringUtils.isEmpty(data)) {
            return new HashMap<>();
        }
        return JsonUtils.deserialize(data, new TypeReference<Map<String, String>>() {
        });

    }

    @JsonProperty("dataTemplateMap")
    @Transient
    public void setDataTemplateMap(Map<String, String> dataTemplateMap) {
        if (MapUtils.isEmpty(dataTemplateMap)) {
            setDataTemplates(null);
            return;
        }
        String data = JsonUtils.serialize(dataTemplateMap);
        setDataTemplates(data);
    }

    public String getDataTemplates() {
        return dataTemplates;
    }

    public void setDataTemplates(String dataTemplates) {
        this.dataTemplates = dataTemplates;
    }

    @JsonProperty("csvAdaptor")
    @Transient
    public CSVAdaptor getCsvAdaptor() {
        if (csvAdaptor == null) {
            return null;
        }
        String uncompressedData = new String(CompressionUtils.decompressByteArray(csvAdaptor));
        if (StringUtils.isNotEmpty(uncompressedData)) {
            return JsonUtils.deserialize(uncompressedData, CSVAdaptor.class);
        } else {
            return null;
        }
    }

    @JsonProperty("csvAdaptor")
    @Transient
    public void setCsvAdaptor(CSVAdaptor csvAdaptor) {
        if (csvAdaptor == null) {
            this.csvAdaptor = null;
            return;
        }
        String string = JsonUtils.serialize(csvAdaptor);
        byte[] payloadData = string.getBytes();
        try {
            byte[] compressedData = CompressionUtils.compressByteArray(payloadData);
            this.csvAdaptor = compressedData;
        } catch (IOException e) {
            log.error("Failed to compress payload [" + csvAdaptor + "]", e);
        }
    }
}
