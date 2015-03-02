package com.latticeengines.skald;

import java.util.Date;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ScoreHistoryEntry {
    public String requestID;
    Date received;
    int duration;

    public CustomerSpace space;
    public String recordID;
    public String match;

    public String request;
    public String result;
    public String error;
}
