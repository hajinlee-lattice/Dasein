package com.latticeengines.datacloud.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;

public interface CategoricalAttributeEntityMgr {

    List<CategoricalAttribute> getChildren(Long parentId);

    CategoricalAttribute getRootAttribute(String source, String dimension);

    CategoricalAttribute getAttribute(Long pid);

}
