package com.latticeengines.workflow.exposed.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.workflow.SourceFile;

public interface SourceFileEntityMgr extends BaseEntityMgr<SourceFile> {

    SourceFile findByName(String name);
}
