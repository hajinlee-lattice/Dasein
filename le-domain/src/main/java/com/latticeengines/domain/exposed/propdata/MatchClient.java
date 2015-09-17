package com.latticeengines.domain.exposed.propdata;

import org.codehaus.jackson.annotate.JsonIgnore;

public enum MatchClient {

    PD126("10.51.15.126"),
    PD127("10.51.15.127"),
    PD128("10.51.15.128"),
    PD131("10.51.15.131"),
    PD130("10.51.15.130"),
    PD238("10.41.1.238");

    private final String host;

    MatchClient(String host) { this.host = host; }

    @JsonIgnore
    public String getHost() { return host; }

}
