package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;

public interface PlayService {

    List<Play> getAllPlays();

    Play createOrUpdate(Play play, String tenantId);

    Play getPlayByName(String name);

    void deleteByName(String name);

}
