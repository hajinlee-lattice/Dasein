package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = ContactMasterTpsColumn.TABLE_NAME, indexes = { //
        @Index(name = "IX_ColumnName", columnList = "ColumnName"), //
        @Index(name = "IX_MatchDestination", columnList = "MatchDestination") })
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContactMasterTpsColumn implements HasPid, Serializable {

    public static final String TABLE_NAME = "ContactMasterTpsColumn";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "ColumnName", nullable = false, length = 100)
    private String columnName;

    @Column(name = "JavaClass", nullable = false, length = 50)
    private String javaClass;

    @Column(name = "IsDerived", nullable = false)
    private boolean isDerived;

    @Column(name = "MatchDestination", nullable = false, length = 100)
    private String matchDestination;

    @Override
    @JsonIgnore
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public void setJavaClass(String javaClass) {
        this.javaClass = javaClass;
    }

    public boolean isDerived() {
        return isDerived;
    }

    public void setDerived(boolean derived) {
        isDerived = derived;
    }

    public String getMatchDestination() {
        return matchDestination;
    }

    public void setMatchDestination(String matchDestination) {
        this.matchDestination = matchDestination;
    }
}
