package com.latticeengines.ulysses.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;

public interface SelectedAttrDao extends BaseDao<SelectedAttribute> {
    Integer count(boolean onlyPremium);
}
