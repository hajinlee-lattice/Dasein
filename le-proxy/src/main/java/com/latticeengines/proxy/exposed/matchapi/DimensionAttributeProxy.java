package com.latticeengines.proxy.exposed.matchapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("dimensionAttributeProxy")
public class DimensionAttributeProxy extends BaseRestApiProxy {

    public DimensionAttributeProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/dimensionattributes");
    }

    public List<CategoricalDimension> getAllDimensions() {
        String url = constructUrl("/dimensions");
        List<?> dimensions = get("getAllDimensions", url, List.class);
        return JsonUtils.convertList(dimensions, CategoricalDimension.class);
    }

    public List<CategoricalAttribute> getAllAttributes(Long rootId) {
        String url = constructUrl("/attributes?rootId={rootId}", rootId);
        List<?> attributes = get("getAllAttributes", url, List.class);

        return JsonUtils.convertList(attributes, CategoricalAttribute.class);
    }

}
