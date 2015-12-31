package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.AmAttribute;

public interface AmAttributeEntityMgr extends BaseEntityMgr<AmAttribute> {
    List<AmAttribute> findAttributes(String key, String parentKey, String parentValue, boolean populate);
}
