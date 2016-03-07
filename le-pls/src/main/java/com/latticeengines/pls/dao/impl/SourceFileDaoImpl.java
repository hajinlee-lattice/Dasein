package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.dao.SourceFileDao;

@Component("sourceFileDao")
public class SourceFileDaoImpl extends BaseDaoImpl<SourceFile> implements SourceFileDao {

    @Override
    protected Class<SourceFile> getEntityClass() {
        return SourceFile.class;
    }

    @Override
    public SourceFile findByName(String name) {
        return findByField("name", name);
    }
}
