package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.query.AttributeLookup;

public interface PlayService {

    List<Play> getAllPlays();

    Play getPlayByName(String name, Boolean considerDeleted);

    Play createOrUpdate(Play play, String tenantId);

    Play createOrUpdate(Play play, boolean shouldLoadCoverage, String tenantId);

    void deleteByName(String name, Boolean hardDelete);

    List<Play> getAllFullPlays(boolean shouldLoadCoverage, String ratingEngineId);

    Play getFullPlayByName(String name, Boolean considerDeleted);

    void publishTalkingPoints(String playName, String customerSpace);

    List<AttributeLookup> findDependingAttributes(List<Play> plays);

    List<Play> findDependingPalys(List<String> attributes);

    List<Play> findDependantPlays(List<String> attributes);

    List<String> getAllDeletedPlayIds(boolean forCleanupOnly);
}
