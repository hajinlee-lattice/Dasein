package com.latticeengines.datafabric.entitymanager.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;

public class SampleFabricEntityMgr extends BaseFabricEntityMgrImpl<SampleEntity> {

    public SampleFabricEntityMgr(Builder builder) {
        super(builder);
    }

    List<SampleEntity> findByLatticeId(String latticeId) {
        Map<String, String> properties = new HashMap<String, String>();
        properties.put("latticeId", latticeId);
        return super.findByProperties(properties);
    }
}
