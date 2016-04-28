package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileEntityMgr extends BaseEntityMgr<SourceFile> {

    SourceFile findByName(String name);

    SourceFile findByApplicationId(String applicationId);
    
    List<SourceFile> findAllSourceFiles();
}
