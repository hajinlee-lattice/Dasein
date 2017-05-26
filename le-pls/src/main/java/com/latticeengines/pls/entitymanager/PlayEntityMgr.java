package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.Play;

public interface PlayEntityMgr extends BaseEntityMgr<Play> {

    void create(Play entity);

    List<Play> findAll();

    Play findByName(String name);

    void deleteByName(String name);

    void createOrUpdate(Play play);

}
