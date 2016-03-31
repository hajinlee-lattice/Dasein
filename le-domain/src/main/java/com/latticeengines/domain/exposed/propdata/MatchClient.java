package com.latticeengines.domain.exposed.propdata;

public enum MatchClient {

    PD126("10.51.15.126", 1433, "PropDataMatchDB", "DLTransfer", "Q1nh4HIYGkg4OnQIEbEuiw=="),
    PD128("10.51.15.128", 1433, "PropDataMatchDB", "DLTransfer", "Q1nh4HIYGkg4OnQIEbEuiw=="),
    PD131("10.51.15.131", 1433, "PropDataMatchDB", "DLTransfer", "Q1nh4HIYGkg4OnQIEbEuiw=="),
    PD144("10.51.15.144", 1433, "PropDataMatchDB", "DLTransfer", "Q1nh4HIYGkg4OnQIEbEuiw=="),
    PD130("10.51.15.130", 1433, "PropDataMatchDB", "DLTransfer", "Q1nh4HIYGkg4OnQIEbEuiw==");

    final String host;
    final int port;
    final String database;
    final String username;
    final String encryptedPassword;

    MatchClient(String host, int port, String database, String username, String encryptedPassword) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.username = username;
        this.encryptedPassword = encryptedPassword;
    }
}
