package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToOne;
import javax.persistence.UniqueConstraint;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@javax.persistence.Table(name = "DATAFEED_TASK_TABLE", uniqueConstraints = @UniqueConstraint(columnNames = {
        "FK_TASK_ID", "FK_TABLE_ID" }))
public class DataFeedTaskTable implements HasPid, Serializable {

    private static final long serialVersionUID = -278909761082585235L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "`FK_TASK_ID`", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private DataFeedTask dataFeedTask;

    @JsonIgnore
    @OneToOne(cascade = { CascadeType.MERGE }, fetch = FetchType.EAGER)
    @JoinColumn(name = "`FK_TABLE_ID`", nullable = false)
    @OnDelete(action = OnDeleteAction.CASCADE)
    private Table table;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public DataFeedTask getFeedTask() {
        return dataFeedTask;
    }

    public void setFeedTask(DataFeedTask dataFeedTask) {
        this.dataFeedTask = dataFeedTask;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }
}
