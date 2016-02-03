package com.latticeengines.scoringapi.history;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "score_history")
public class ScoreHistoryEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public int id;

    @Column(nullable = false)
    public String space;
    @Column(name = "record_id")
    public String recordID;

    @Column(name = "request_id", nullable = false)
    public String requestID;
    @Column(nullable = false)
    long received;
    @Column(nullable = false)
    long duration;

    @Column(name = "model_name")
    public String modelName;
    @Column(name = "model_version")
    public Integer modelVersion;

    @Column
    public String totality;

    @Column
    public String request;
    @Column
    public String response;
}
