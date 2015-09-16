package com.latticeengines.propdata.api.datasource;

public enum MatcherClient {

    PD126("10.51.15.126"),
    PD127("10.51.15.127"),
    PD128("10.51.15.128"),
    PD131("10.51.15.131"),
    PD130("10.51.15.130");

    private final String host;

    MatcherClient(String host) { this.host = host; }

    public String getHost() { return host; }

}
