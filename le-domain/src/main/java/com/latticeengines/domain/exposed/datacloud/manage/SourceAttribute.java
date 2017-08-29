package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.Index;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "SourceAttribute", uniqueConstraints = {
        @UniqueConstraint(columnNames = { "Source", "Stage", "Transformer", "Attribute", "DataCloudVersion" }) })
public class SourceAttribute implements HasPid, Serializable {

    private static final long serialVersionUID = 5143418326245069059L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "SourceAttributeID", unique = true, nullable = false)
    private Long sourceAttributeId;

    @Index(name = "IX_SOURCE_STAGE_TRANSFORMER")
    @Column(name = "Source", nullable = false, length = 128)
    private String source;

    @Index(name = "IX_SOURCE_STAGE_TRANSFORMER")
    @Column(name = "Stage", nullable = false, length = 32)
    private String stage;

    @Index(name = "IX_SOURCE_STAGE_TRANSFORMER")
    @Column(name = "Transformer", nullable = false, length = 32)
    private String transformer;

    @Column(name = "Attribute", nullable = false, length = 128)
    private String attribute;

    @Column(name = "Arguments", length = 1024)
    private String arguments;

    @Column(name = "DataCloudVersion", length = 50)
    private String dataCloudVersion;

    public SourceAttribute() {
        super();
    }

    public Long getSourceAttributeId() {
        return sourceAttributeId;
    }

    public void setSourceAttributeId(Long sourceAttributeId) {
        this.sourceAttributeId = sourceAttributeId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getStage() {
        return stage;
    }

    public void setStage(String stage) {
        this.stage = stage;
    }

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer; 
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getArguments() {
        return arguments;
    }

    public void setArguments(String arguments) {
        this.arguments = arguments;
    }

    @Override
    public Long getPid() {
        return getSourceAttributeId();
    }

    @Override
    public void setPid(Long pid) {
        setSourceAttributeId(pid);
    }
}
