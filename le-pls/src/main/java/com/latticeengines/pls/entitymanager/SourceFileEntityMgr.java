package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.SourceFile;

public interface SourceFileEntityMgr extends BaseEntityMgr<SourceFile> {

    SourceFile findByName(String name);
}
