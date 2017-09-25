package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;

public interface PlayService {

    List<Play> getAllPlays();

    Play getPlayByName(String name);

    Play createOrUpdate(Play play, String tenantId);

    void deleteByName(String name);

    List<Play> getAllFullPlays(boolean shouldLoadCoverage, String ratingEngineId);

    Play getFullPlayByName(String name);

    void publishTalkingPoints(String playName, String customerSpace);
}
