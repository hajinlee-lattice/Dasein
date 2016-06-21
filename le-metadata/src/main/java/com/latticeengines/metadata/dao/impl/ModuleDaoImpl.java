package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.metadata.dao.ModuleDao;

@Component("moduleDao")
public class ModuleDaoImpl extends BaseDaoImpl<Module> implements ModuleDao {

    @Override
    protected Class<Module> getEntityClass() {
        return Module.class;
    }

}
