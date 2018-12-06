package com.latticeengines.domain.exposed.datacloud.match;

public enum MatchActorType {
    ANCHOR("AnchorActor"), //
    MICRO_ENGINE("MicroEngineActor"), //
    JUNCION("JunctionActor");
    
    private final String name;

    MatchActorType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
