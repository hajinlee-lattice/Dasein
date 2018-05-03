package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Map;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public interface CDLAttrConfigProxy {

    AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity, boolean render);

    AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName);

    AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request);

    AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request);

    List<AttrConfigOverview<?>> getAttrConfigOverview(String customerSpace, String categoryName,
            @NonNull String propertyName);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace,
            @Nullable List<String> categoryNames, @NonNull List<String> propertyNames, boolean activeOnly);

}
