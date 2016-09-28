package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SourceFileEntityMgr extends BaseEntityMgr<SourceFile> {

    SourceFile findByName(String name);

    SourceFile findByApplicationId(String applicationId);

    List<SourceFile> findAllSourceFiles();

    SourceFile findByTableName(String tableName);

    SourceFile getByTableName(String tableName);

    void create(SourceFile sourceFile, Tenant tenant);
}
