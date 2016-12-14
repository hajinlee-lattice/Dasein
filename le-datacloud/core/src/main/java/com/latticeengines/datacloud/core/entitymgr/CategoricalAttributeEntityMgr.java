package com.latticeengines.datacloud.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

public interface CategoricalAttributeEntityMgr {

    List<CategoricalAttribute> getChildren(Long parentId);

    CategoricalAttribute getRootAttribute(String source, String dimension);

    CategoricalAttribute getAttribute(Long pid);

    CategoricalAttribute getAttribute(String attrName, String attrValue);

    List<CategoricalDimension> getAllDimensions();

    List<CategoricalAttribute> getAllAttributes(Long rootId);

}
