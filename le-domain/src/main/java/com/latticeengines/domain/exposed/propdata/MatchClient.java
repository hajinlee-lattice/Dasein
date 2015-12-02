package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = MatchClientSerializer.class)
public enum MatchClient {

    PD126("10.51.15.126", 1433),
    PD127("10.51.15.127", 1433),
    PD128("10.51.15.128", 1433),
    PD131("10.51.15.131", 1433),
    PD130("10.51.15.130", 1433),
    PD238("10.41.1.238", 1437);

    private final String host;
    private final int port;

    MatchClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() { return host; }

    public int getPort() { return port; }
}
