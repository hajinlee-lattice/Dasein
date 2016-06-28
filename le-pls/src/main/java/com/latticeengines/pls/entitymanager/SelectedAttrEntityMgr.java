package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.SelectedAttribute;

public interface SelectedAttrEntityMgr {

    List<SelectedAttribute> findAll();

    List<SelectedAttribute> upsert(List<SelectedAttribute> attributes);

}
