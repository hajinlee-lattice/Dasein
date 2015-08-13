package com.latticeengines.release.exposed.domain;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component("statusContext")
@Scope("prototype")
public class StatusContext {

    private String responseMessage = "";

    private int statusCode = -1;

    public String getResponseMessage() {
        return this.responseMessage;
    }

    public void setResponseMessage(String responseMessage) {
        this.responseMessage = responseMessage;
    }

    public int getStatusCode() {
        return this.statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public boolean stopProcess() {
        if (statusCode >= 400 && statusCode < 600)
            return true;
        if (responseMessage.toLowerCase().contains("fail") || responseMessage.toLowerCase().contains("error"))
            return true;
        return false;
    }

}
