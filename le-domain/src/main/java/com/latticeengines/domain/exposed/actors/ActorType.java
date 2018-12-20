package com.latticeengines.domain.exposed.actors;

public enum ActorType {
    ANCHOR("AnchorActor"), //
    MICRO_ENGINE("MicroEngineActor"), //
    JUNCION("JunctionActor");
    
    private final String name;

    ActorType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

}
