package com.latticeengines.proxy.exposed.matchapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.network.exposed.propdata.DimensionAttributenterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("dimensionAttributeProxy")
public class DimensionAttributeProxy extends BaseRestApiProxy implements DimensionAttributenterface {

    public DimensionAttributeProxy() {
        super(PropertyUtils.getProperty("common.matchapi.url"), "/match/dimensionattributes");
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public List<CategoricalDimension> getAllDimensions() {
        String url = constructUrl("/dimensions");
        return get("getAllDimensions", url, List.class);
    }

    @Override
    @SuppressWarnings({ "unchecked" })
    public List<CategoricalAttribute> getAllAttributes(Long rootId) {
        String url = constructUrl("/attributes?rootId={rootId}", rootId);
        return get("getAllAttributes", url, List.class);
    }

}
