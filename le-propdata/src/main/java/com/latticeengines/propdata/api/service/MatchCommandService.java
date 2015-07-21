package com.latticeengines.propdata.api.service;

public interface MatchCommandService {

    Long createMatchCommand(String sourceTable, String destTables,
            String contractExternalID, String matchClient);

    String getMatchCommandStatus(String commandID, String matchClient);

}
