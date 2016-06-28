package com.latticeengines.pls.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.pls.dao.SelectedAttrDao;
import org.springframework.stereotype.Component;

@Component("enrichmentAttrDao")
public class SelectedAttrDaoImpl extends BaseDaoImpl<SelectedAttribute> implements SelectedAttrDao {

    @Override
    protected Class<SelectedAttribute> getEntityClass() {
        return SelectedAttribute.class;
    }
}
