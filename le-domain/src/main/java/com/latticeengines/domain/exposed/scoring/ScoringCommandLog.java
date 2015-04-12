package com.latticeengines.domain.exposed.scoring;

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
@Table(name = "ScoringCommandLog")
public class ScoringCommandLog extends AbstractTimestampEntity implements HasPid, Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne()
    @JoinColumn(name = "LeadInputQueue_ID", nullable = false)
    private ScoringCommand scoringCommand;

    @Column(name = "Message", nullable = false, length = 65535)
    private String message;

    ScoringCommandLog() {
        super();
    }

    public ScoringCommandLog(ScoringCommand scoringCommand, String message) {
        super();
        this.scoringCommand = scoringCommand;
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

    public ScoringCommand getScoringCommand() {
        return scoringCommand;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return message;
    }
}
