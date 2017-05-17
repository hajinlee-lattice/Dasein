package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;

public interface PlayEntityMgr {

    void create(Play entity);

    List<Play> findAll();

    Play findByName(String name);

    List<Play> findAllVisible();

    void deleteByName(String name);

}
