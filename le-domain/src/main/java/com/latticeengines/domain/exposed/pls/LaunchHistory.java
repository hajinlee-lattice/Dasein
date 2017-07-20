package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LaunchHistory {

    @JsonProperty("PlayLaunch")
    private PlayLaunch playLaunch;

    @JsonProperty("NewContactsNum")
    private Long newContactsNum;

    @JsonProperty("NewAccountsNum")
    private Long newAccountsNum;

    public void setNewContactsNum(Long newContactsNum) {
        this.newContactsNum = newContactsNum;
    }

    public Long getNewContactsNum() {
        return this.newContactsNum;
    }

    public void setNewAccountsNum(Long newAccountsNum) {
        this.newAccountsNum = newAccountsNum;
    }

    public Long getNewAccountsNum() {
        return this.newAccountsNum;
    }

    public PlayLaunch getPlayLaunch() {
        return this.playLaunch;
    }

    public void setPlayLaunch(PlayLaunch playLaunch) {
        this.playLaunch = playLaunch;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
