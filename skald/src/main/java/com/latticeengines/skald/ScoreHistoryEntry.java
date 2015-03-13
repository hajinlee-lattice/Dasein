package com.latticeengines.skald;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "ScoreHistory")
public class ScoreHistoryEntry {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public int id;

    @Column(nullable = false)
    public String space;
    @Column
    public String recordID;

    @Column(nullable = false)
    public String requestID;
    @Column(nullable = false)
    long received;
    @Column(nullable = false)
    long duration;

    @Column
    public String modelName;
    @Column
    public int modelVersion;

    @Column
    public String totality;

    @Column
    public String request;
    @Column
    public String response;
}
