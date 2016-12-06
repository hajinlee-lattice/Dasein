package com.latticeengines.datacloud.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

public interface TestCategoricalAttributeEntityMgr {

    CategoricalDimension addDimension(CategoricalDimension dimension);
    CategoricalAttribute addAttribute(CategoricalAttribute attribute);
    void removeAttribute(Long pid);
    void removeDimension(Long pid);
    List<CategoricalAttribute> allAttributes();
    List<CategoricalDimension> allDimensions();
}
