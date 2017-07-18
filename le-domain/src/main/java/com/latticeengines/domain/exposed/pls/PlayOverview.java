package com.latticeengines.domain.exposed.pls;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class PlayOverview {

    @JsonProperty("Play")
    private Play play;

    @JsonProperty("LaunchHistory")
    private LaunchHistory launchHistory;

    @JsonProperty("AccountRatings")
    private Map<BucketName, Integer> accountRatingMap;

    @JsonProperty("TalkingPoints")
    private List<TalkingPointDTO> talkingPoints;

    public Play getPlay() {
        return this.play;
    }

    public void setPlay(Play play) {
        this.play = play;
    }

    public LaunchHistory getLaunchHistory() {
        return this.launchHistory;
    }

    public void setLaunchHistory(LaunchHistory launchHistory) {
        this.launchHistory = launchHistory;
    }

    public void setAccountRatingMap(Map<BucketName, Integer> accountRatingMap) {
        this.accountRatingMap = accountRatingMap;
    }

    public Map<BucketName, Integer> getAccountRatingMap() {
        return this.accountRatingMap;
    }

    public void setTalkingPoints(List<TalkingPointDTO> talkingPoints) {
        this.talkingPoints = talkingPoints;
    }

    public List<TalkingPointDTO> getTalkingPoints() {
        return this.talkingPoints;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
