package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public interface PlayService {

    List<Play> getAllPlays();

    Play getPlayByName(String name);

    Play createOrUpdate(Play play, String tenantId);

    void deleteByName(String name);

    List<Play> getAllFullPlays(boolean shouldLoadCoverage, String ratingEngineId);

    Play getFullPlayByName(String name);

    void publishTalkingPoints(String playName, String customerSpace);

    List<AttributeLookup> findDependingAttributes (List<Play> plays);

    List<Play> findDependingPalys(List<String> attributes);
}
