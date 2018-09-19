package com.latticeengines.pls.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    @Inject
    private ActionProxy actionProxy;

    @Override
    public Action create(Action action) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return actionProxy.createAction(tenantId, action);
    }

}
