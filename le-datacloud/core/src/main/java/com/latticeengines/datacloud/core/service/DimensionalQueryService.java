package com.latticeengines.datacloud.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;

public interface DimensionalQueryService {

    Long findAttrId (DimensionalQuery query);

    List<CategoricalDimension> getAllDimensions();

    List<CategoricalAttribute> getAllAttributes(Long rootId);

}
