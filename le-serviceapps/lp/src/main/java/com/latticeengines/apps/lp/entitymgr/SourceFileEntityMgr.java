package com.latticeengines.apps.lp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SourceFileEntityMgr extends BaseEntityMgrRepository<SourceFile, Long> {

    SourceFile findByName(String name);

    SourceFile findByNameFromWriter(String name);

    SourceFile findByApplicationId(String applicationId);

    List<SourceFile> findAllSourceFiles();

    SourceFile findByTableName(String tableName);

    SourceFile getByTableName(String tableName);

    void create(SourceFile sourceFile, Tenant tenant);

    SourceFile findByWorkflowPid(Long workflowPid);
}
