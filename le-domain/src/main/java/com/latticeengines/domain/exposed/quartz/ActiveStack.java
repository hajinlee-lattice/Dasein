package com.latticeengines.domain.exposed.quartz;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Table(name = "QUARTZ_ACTIVESTACK")
public class ActiveStack implements HasPid {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @JsonIgnore
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @JsonProperty("active_stack")
    @Column(name = "ACTIVESTACK", nullable = false)
    private String activeStack;
    
    public String getActiveStack() {
        return activeStack;
    }
    
    public void setActiveStack(String activeStack) {
        this.activeStack = activeStack;
    }

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
        
    }

}
