package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileDao extends BaseDao<SourceFile> {

    SourceFile findByName(String name);

}
