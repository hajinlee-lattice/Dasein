package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.pls.SelectedAttribute;

public interface SelectedAttrEntityMgr {

    List<SelectedAttribute> findAll();

    List<SelectedAttribute> upsert(List<SelectedAttribute> attributes);

    List<SelectedAttribute> add(List<SelectedAttribute> newAttrList);

    List<SelectedAttribute> delete(List<SelectedAttribute> dropAttrList);

    Integer count(boolean onlyPremium);

}
