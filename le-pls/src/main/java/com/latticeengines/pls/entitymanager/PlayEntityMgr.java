package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Play;

public interface PlayEntityMgr extends BaseEntityMgr<Play> {

    Play createOrUpdatePlay(Play entity);

    List<Play> findAll();

    List<Play> findAllByRatingEnginePid(long pid);

    Play findByName(String name);

    void deleteByName(String name);

    void createOrUpdate(Play play);

}
