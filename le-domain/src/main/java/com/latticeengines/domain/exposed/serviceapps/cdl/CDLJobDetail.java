package com.latticeengines.domain.exposed.serviceapps.cdl;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "CDL_JOB_DETAIL")
public class CDLJobDetail implements HasPid, Serializable {

    private static final long serialVersionUID = 7530709453299469324L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @JsonProperty("cdl_job_type")
    @Column(name = "CDL_JOB_TYPE", nullable = false)
    @Enumerated(EnumType.STRING)
    private CDLJobType cdlJobType;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public CDLJobType getCdlJobType() {
        return cdlJobType;
    }

    public void setCdlJobType(CDLJobType cdlJobType) {
        this.cdlJobType = cdlJobType;
    }
}
