package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class LaunchHistory {

    @JsonProperty("newContactsNum")
    private Long newContactsNum;

    @JsonProperty("newAccountsNum")
    private Long newAccountsNum;

    @JsonProperty("mostRecentLaunch")
    private PlayLaunch mostRecentLaunch;

    @JsonProperty("lastIncompleteLaunch")
    private PlayLaunch lastIncompleteLaunch;

    @JsonProperty("lastCompletedLaunch")
    private PlayLaunch lastCompletedLaunch;

    public Long getNewContactsNum() {
        return this.newContactsNum;
    }

    public void setNewContactsNum(Long newContactsNum) {
        this.newContactsNum = newContactsNum;
    }

    public Long getNewAccountsNum() {
        return this.newAccountsNum;
    }

    public void setNewAccountsNum(Long newAccountsNum) {
        this.newAccountsNum = newAccountsNum;
    }

    public PlayLaunch getMostRecentLaunch() {
        return mostRecentLaunch;
    }

    public void setMostRecentLaunch(PlayLaunch mostRecentLaunch) {
        this.mostRecentLaunch = mostRecentLaunch;
    }

    public PlayLaunch getLastIncompleteLaunch() {
        return this.lastIncompleteLaunch;
    }

    public void setLastIncompleteLaunch(PlayLaunch lastIncompleteLaunch) {
        this.lastIncompleteLaunch = lastIncompleteLaunch;
    }

    public PlayLaunch getLastCompletedLaunch() {
        return this.lastCompletedLaunch;
    }

    public void setLastCompletedLaunch(PlayLaunch lastCompletedLaunch) {
        this.lastCompletedLaunch = lastCompletedLaunch;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
