package com.latticeengines.spark.service.impl;

import java.util.EnumSet;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
class LivyStatement {

    @JsonProperty
    Integer id;

    @JsonProperty
    String code;

    @JsonProperty
    State state;

    @JsonProperty
    Double progress;

    @JsonProperty
    Output output;

    enum State {
        waiting,
        running,
        available,
        error,
        cancelling,
        cancelled;

        static EnumSet<State> TERMINAL_STATES = EnumSet.of(available, error, cancelled);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    static class Output {

        @JsonProperty
        String status;

        @JsonProperty
        Integer execution_count;

        @JsonProperty
        JsonNode data;

        @JsonProperty
        String ename;

        @JsonProperty
        String evalue;

        @JsonProperty
        List<String> traceback;

    }

}
