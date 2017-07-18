package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayOverview;

public interface PlayService {

    List<Play> getAllPlays();

    List<PlayOverview> getAllPlayOverviews();

    Play createOrUpdate(Play play, String tenantId);

    Play getPlayByName(String name);

    PlayOverview getPlayOverviewByName(String name);

    void deleteByName(String name);

}
