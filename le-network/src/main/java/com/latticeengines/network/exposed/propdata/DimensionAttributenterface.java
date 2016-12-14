package com.latticeengines.network.exposed.propdata;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

public interface DimensionAttributenterface {

    List<CategoricalDimension> getAllDimensions();

    List<CategoricalAttribute> getAllAttributes(Long rootId);

}
