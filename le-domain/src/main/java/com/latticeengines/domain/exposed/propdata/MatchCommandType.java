package com.latticeengines.domain.exposed.propdata;

public enum MatchCommandType {
    MATCH_WITH_UNIVERSE("RunMatchWithLEUniverse");

    private String commandName;

    MatchCommandType(String commandName) {
        this.commandName = commandName;
    }

    public String getCommandName() { return this.commandName; }

}
