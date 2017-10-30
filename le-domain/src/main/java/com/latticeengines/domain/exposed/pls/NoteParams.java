package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class NoteParams {

    @JsonProperty("user_name")
    private String userName;

    @JsonProperty("content")
    private String content;

    @JsonProperty("origin")
    private String origin = NoteOrigin.NOTE.name();

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getOrigin() {
        return this.origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

}
