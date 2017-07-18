package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LaunchHistory {

    @JsonProperty("LastContactsNum")
    private Long lastContactsNum;

    @JsonProperty("NewContactsNum")
    private Long newContactsNum;

    @JsonProperty("LastAccountsNum")
    private Long lastAccountsNum;

    @JsonProperty("NewAccountsNum")
    private Long newAccountsNum;

    public void setLastContactsNum(Long lastContactsNum) {
        this.lastContactsNum = lastContactsNum;
    }

    public Long getLastContactsNum() {
        return this.lastContactsNum;
    }

    public void setNewContactsNum(Long newContactsNum) {
        this.newContactsNum = newContactsNum;
    }

    public Long getNewContactsNum() {
        return this.newContactsNum;
    }

    public void setLastAccountsNum(Long lastAccountsNum) {
        this.lastAccountsNum = lastAccountsNum;
    }

    public Long getLastAccountsNum() {
        return this.lastAccountsNum;
    }

    public void setNewAccountsNum(Long newAccountsNum) {
        this.newAccountsNum = newAccountsNum;
    }

    public Long getNewAccountsNum() {
        return this.newAccountsNum;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
