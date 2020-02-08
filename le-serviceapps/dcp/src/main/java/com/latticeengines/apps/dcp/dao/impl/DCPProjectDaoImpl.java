package com.latticeengines.apps.dcp.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.DCPProjectDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.dcp.DCPProject;

@Component("dcpProjectDao")
public class DCPProjectDaoImpl extends BaseDaoImpl<DCPProject> implements DCPProjectDao {

    @Override
    protected Class<DCPProject> getEntityClass() {
        return DCPProject.class;
    }
}
