package com.latticeengines.release.exposed.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Notification {

    private String color = "green";
    private String messageFormat = "text";
    private String message;

    public Notification(String color, String message) {
        this.color = color;
        this.message = message;
    }

    @JsonProperty("color")
    public String getColor() {
        return this.color;
    }

    @JsonProperty("color")
    public void setColor(String color) {
        this.color = color;
    }

    @JsonProperty("message_format")
    public String getMessageFormat() {
        return this.messageFormat;
    }

    @JsonProperty("message_format")
    public void setMessageFormat(String messageFormat) {
        this.messageFormat = messageFormat;
    }

    @JsonProperty("message")
    public String getMessage() {
        return this.message;
    }

    @JsonProperty("message")
    public void setMessage(String message) {
        this.message = message;
    }
}
