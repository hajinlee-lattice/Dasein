package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.metadata.entitymgr.DependableObjectEntityMgr;
import com.latticeengines.metadata.service.DependableObjectService;

@Component("dependableObjectService")
public class DependableObjectServiceImpl implements DependableObjectService {
    @Autowired
    private DependableObjectEntityMgr dependableObjectEntityMgr;

    @Override
    public DependableObject find(String customerSpace, String type, String name) {
        return dependableObjectEntityMgr.find(type, name);
    }

    @Override
    public void createOrUpdate(String customerSpace, DependableObject dependableObject) {
        DependableObject existing = find(customerSpace, dependableObject.getType(), dependableObject.getName());
        if (existing != null) {
            dependableObjectEntityMgr.delete(existing);
        }
        dependableObjectEntityMgr.create(dependableObject);
    }

    @Override
    public void delete(String customerSpace, String type, String name) {
        dependableObjectEntityMgr.delete(type, name);
    }
}
