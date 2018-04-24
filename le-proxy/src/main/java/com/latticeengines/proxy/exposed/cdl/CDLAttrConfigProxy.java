package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

import org.springframework.lang.NonNull;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public interface CDLAttrConfigProxy {

    AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity, boolean render);

    AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName);

    AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request);

    AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request);

    List<AttrConfigOverview<?>> getAttrConfigOverview(String customerSpace, String categoryName,
            @NonNull String propertyName);

}
