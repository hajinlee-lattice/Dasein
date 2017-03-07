package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import org.hibernate.annotations.Index;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@javax.persistence.Table(name = "METADATA_DATA_COLLECTION_PROPERTY")
public class DataCollectionProperty implements HasOptionAndValue, HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne
    @JoinColumn(name = "FK_DATA_COLLECTION_ID", nullable = false)
    @JsonIgnore
    private DataCollection dataCollection;

    @Column(name = "PROPERTY", nullable = false)
    @JsonProperty("property")
    @Index(name = "DATA_COLLECTION_PROPERTY_IDX")
    private String property;

    @Column(name = "VALUE", nullable = true, length = 2048)
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

    public DataCollection getDataCollection() {
        return dataCollection;
    }

    public void setDataCollection(DataCollection dataCollection) {
        this.dataCollection = dataCollection;
    }
}
