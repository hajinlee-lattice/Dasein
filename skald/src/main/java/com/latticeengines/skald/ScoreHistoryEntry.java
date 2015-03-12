package com.latticeengines.skald;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ScoreHistoryEntry {
    public String requestID;
    long received;
    long duration;

    public CustomerSpace space;
    public String recordID;

    public String modelName;
    public int modelVersion;

    public String totality;

    public String request;
    public String response;
}
