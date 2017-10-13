package com.latticeengines.domain.exposed.dante;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public class DanteLeadDTO {
    private Recommendation recommendation;
    private Play play;
    private PlayLaunch playLaunch;

    public DanteLeadDTO() {
    }

    public DanteLeadDTO(Recommendation recommendation, Play play, PlayLaunch playLaunch) {
        this.recommendation = recommendation;
        this.play = play;
        this.playLaunch = playLaunch;
    }

    public Recommendation getRecommendation() {
        return recommendation;
    }

    public void setRecommendation(Recommendation recommendation) {
        this.recommendation = recommendation;
    }

    public Play getPlay() {
        return play;
    }

    public void setPlay(Play play) {
        this.play = play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    public void setPlayLaunch(PlayLaunch playLaunch) {
        this.playLaunch = playLaunch;
    }
}
