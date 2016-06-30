package com.latticeengines.domain.exposed.modelreview;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.MappedSuperclass;
import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.metadata.Table;

@MappedSuperclass
public abstract class BaseRuleResult implements HasPid{

    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    protected Long pid;

    @JsonProperty
    @Transient
    protected int flaggedItemCount;

    @JsonProperty
    @Column(name = "RULENAME", nullable = false)
    protected String dataRuleName;

    @JsonIgnore
    @ManyToOne(cascade = { CascadeType.MERGE, CascadeType.REMOVE })
    @JoinColumn(name = "FK_TABLE_ID", nullable = false)
    protected Table table;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public int getFlaggedItemCount() {
        return flaggedItemCount;
    }

    public void setFlaggedItemCount(int flaggedItemCount) {
        this.flaggedItemCount = flaggedItemCount;
    }

    public String getDataRuleName() {
        return dataRuleName;
    }

    public void setDataRuleName(String dataRuleName) {
        this.dataRuleName = dataRuleName;
    }

}
