package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileDao extends BaseDao<SourceFile> {

    SourceFile findByName(String name);

    SourceFile findByApplicationId(String applicationId);
    
    List<SourceFile> findAllSourceFiles();

}
