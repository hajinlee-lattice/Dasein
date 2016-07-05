package com.latticeengines.domain.exposed.datafabric;

public enum TopicScope {

    PRIVATE, STACK_PRIVATE, PUBLIC;

    public static TopicScope fromName(String name) {
        return TopicScope.valueOf(name.toUpperCase());
    }

}
