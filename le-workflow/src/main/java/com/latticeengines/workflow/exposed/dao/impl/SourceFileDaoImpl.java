package com.latticeengines.workflow.exposed.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.latticeengines.workflow.exposed.dao.SourceFileDao;

@Component("sourceFileDao")
public class SourceFileDaoImpl extends BaseDaoImpl<SourceFile> implements SourceFileDao {

    @Override
    protected Class<SourceFile> getEntityClass() {
        return SourceFile.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public SourceFile findByName(String name) {
        return findByField("name", name);
    }
}
