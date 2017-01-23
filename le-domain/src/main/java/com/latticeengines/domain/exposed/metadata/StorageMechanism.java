package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@javax.persistence.Table(name = "METADATA_STORAGE")
@DiscriminatorColumn(name = "NAME", discriminatorType = DiscriminatorType.STRING)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @JsonSubTypes.Type(value = HdfsStorage.class, name = "hdfsStorage"), //
        @JsonSubTypes.Type(value = JdbcStorage.class, name = "jdbcStorage"), //
        @JsonSubTypes.Type(value = NoSQLStorage.class, name = "noSQLStorage"), //
})
public abstract class StorageMechanism implements HasPid, HasName {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;


    @Column(name = "TABLE_NAME", unique = false, nullable = false)
    @JsonProperty("table_name")
    private String tableNameInStorage;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    @JsonIgnore
    private Table table = null;
    
    @Override
    public String getName() {
        DiscriminatorValue val = this.getClass().getAnnotation(DiscriminatorValue.class);
        return val == null ? null : val.value();
    }

    @Override
    public void setName(String name) {
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public void setTable(Table table) {
        this.table = table;
    }
    
    public Table getTable() {
        return table;
    }

    public String getTableNameInStorage() {
        return tableNameInStorage;
    }

    public void setTableNameInStorage(String tableNameInStorage) {
        this.tableNameInStorage = tableNameInStorage;
    }
}
