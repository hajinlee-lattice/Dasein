package com.latticeengines.apps.core.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.dao.DropBoxDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.cdl.DropBox;

@Component("dropBoxDao")
public class DropBoxDaoImpl extends BaseDaoImpl<DropBox> implements DropBoxDao {

    @Override
    protected Class<DropBox> getEntityClass() {
        return DropBox.class;
    }

}
