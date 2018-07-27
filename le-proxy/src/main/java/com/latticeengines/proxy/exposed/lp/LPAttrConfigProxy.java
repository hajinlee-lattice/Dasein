package com.latticeengines.proxy.exposed.lp;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;

public interface LPAttrConfigProxy {

    AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity, boolean render);

    AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName);

    AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request, AttrConfigUpdateMode mode);

    AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request, AttrConfigUpdateMode mode);

}
