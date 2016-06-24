package com.latticeengines.propdata.match.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;

public interface ExternalColumnEntityMgr {

    List<ExternalColumn> findByTag(String tag);

    List<ExternalColumn> findAll();

    ExternalColumn findById(String externalColumnId);

}
