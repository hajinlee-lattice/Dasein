package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AccountEventDetail extends EventDetail {

    public AccountEventDetail() {
        super("Account");
    }

    @JsonProperty("account_id")
    private String accountId;

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }
}
