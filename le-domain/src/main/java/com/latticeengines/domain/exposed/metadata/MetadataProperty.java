package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@MappedSuperclass
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
public abstract class MetadataProperty<T> implements HasOptionAndValue, HasPid {

    public MetadataProperty() {}

    public MetadataProperty(String property, String value) {
        this.property = property;
        this.value = value;
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Column(name = "PID", nullable = false)
    private Long pid;

    @Column(name = "PROPERTY", nullable = false)
    @Index(name = "IX_PROPERTY")
    @JsonProperty("property")
    private String property;

    @Column(name = "VALUE", length = 2048)
    @JsonProperty("value")
    private String value;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    public String getOption() {
        return property;
    }

    @Override
    public void setOption(String property) {
        this.property = property;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public abstract Class<T> getOwnerEntity();

}
