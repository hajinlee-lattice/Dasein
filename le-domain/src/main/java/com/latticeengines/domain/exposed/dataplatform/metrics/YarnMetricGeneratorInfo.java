package com.latticeengines.domain.exposed.dataplatform.metrics;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "DATAPLATFORM_YARNMETRIC_GENERATORINFO")
public class YarnMetricGeneratorInfo implements HasPid, HasName {

    private Long pid;
    private String name;
    private Long finishedTimeBegin;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @Column(name = "NAME", unique = true, nullable = false)
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Column(name = "FINISHED_TIMEBEGIN", nullable = false)
    public Long getFinishedTimeBegin() {
        return finishedTimeBegin;
    }

    public void setFinishedTimeBegin(Long finishedTimeBegin) {
        this.finishedTimeBegin = finishedTimeBegin;
    }

}
