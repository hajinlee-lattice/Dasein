package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

import java.io.Serializable;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.dataplatform.jpa.AbstractTimestampEntity;

@Entity
@Access(AccessType.FIELD)
@Table(name = "LeadScoringCommandLog")
public class ModelCommandLog extends AbstractTimestampEntity implements HasPid, Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;
    
    @ManyToOne()
    @JoinColumn(name = "CommandId", nullable = false)
    private ModelCommand modelCommand;    
    
    @Column(name = "Message", nullable = false, length = 65535)
    private String message;

    ModelCommandLog() {
        super();
    }
    
    public ModelCommandLog(ModelCommand modelCommand, String message) {
        super();
        this.modelCommand = modelCommand;
        this.message = message;
    }
   
    @Override
    public Long getPid() {
        return this.pid;
    }

    @Override
    public void setPid(Long id) {
        this.pid = id;
    }

    public ModelCommand getModelCommand() {
        return modelCommand;
    }

    public String getMessage() {
        return message;
    }
}
