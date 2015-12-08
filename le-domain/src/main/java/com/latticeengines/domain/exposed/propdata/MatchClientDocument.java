package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MatchClientDocument {

    private String host;
    private int port;
    private String database;
    private String username;
    private String encryptedPassword;
    private String url;
    private MatchClient matchClient;

    // json constructor
    private MatchClientDocument(){ }

    public MatchClientDocument(MatchClient matchClient) {
        this.matchClient = matchClient;
        this.host = matchClient.host;
        this.port = matchClient.port;
        this.database = matchClient.database;
        this.url = String.format("jdbc:sqlserver://%s:%d;databaseName=%s;user=$$USER$$;password=$$PASSWD$$",
                this.getHost(), this.getPort(), this.getDatabase());
        this.username = matchClient.username;
        this.encryptedPassword = matchClient.encryptedPassword;
    }

    @JsonProperty("Host")
    public String getHost() { return host; }

    @JsonProperty("Port")
    public int getPort() { return port; }

    @JsonProperty("Database")
    public String getDatabase() { return database; }

    @JsonProperty("Username")
    public String getUsername() { return username; }

    @JsonProperty("EncryptedPassword")
    public String getEncryptedPassword() { return encryptedPassword; }

    @JsonProperty("Url")
    public String getUrl() { return url; }

    @JsonProperty("MatchClient")
    public MatchClient getMatchClient() { return matchClient; }
}
