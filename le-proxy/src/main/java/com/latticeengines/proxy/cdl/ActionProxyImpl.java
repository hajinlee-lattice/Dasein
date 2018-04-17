package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;

@Component("actionProxy")
public class ActionProxyImpl extends MicroserviceRestApiProxy implements ActionProxy {

    protected ActionProxyImpl() {
        super("cdl");
    }

    @Override
    public Action createAction(String customerSpace, Action action) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions", //
                shortenCustomerSpace(customerSpace));
        return post("create action", url, action, Action.class);
    }

    @Override
    public Action updateAction(String customerSpace, Action action) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions", //
                shortenCustomerSpace(customerSpace));
        return put("update action", url, action, Action.class);
    }

    @Override
    public List<Action> getActions(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions", //
                shortenCustomerSpace(customerSpace));
        List list = get("get actions", url, List.class);
        return JsonUtils.convertList(list, Action.class);
    }

    @Override
    public List<Action> getActionsByOwnerId(String customerSpace, Long ownerId) {
        String url;
        if (ownerId == null) {
            url = constructUrl("/customerspaces/{customerSpace}/actions?nullOwnerId=1",
                    shortenCustomerSpace(customerSpace));
        } else {
            url = constructUrl("/customerspaces/{customerSpace}/actions?ownerId={ownerId}",
                    shortenCustomerSpace(customerSpace), ownerId);
        }

        List list = get("get actions by owner", url, List.class);
        return JsonUtils.convertList(list, Action.class);
    }

    @Override
    public List<Action> getActionsByPids(String customerSpace, List<Long> actionPids) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions", //
                shortenCustomerSpace(customerSpace));
        if (CollectionUtils.isNotEmpty(actionPids)) {
            url += "?pids=" + StringUtils.join(actionPids, ",");
        }
        List list = get("get actions by pids", url, List.class);
        return JsonUtils.convertList(list, Action.class);
    }

    @Override
    public void patchOwnerIdByPids(String customerSpace, Long ownerId, List<Long> actionPids) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions/ownerid", //
                shortenCustomerSpace(customerSpace));
        if (ownerId != null || CollectionUtils.isNotEmpty(actionPids)) {
            List<String> params = new ArrayList<>();
            if (ownerId != null) {
                params.add("ownerId=" + ownerId);
            }
            if (CollectionUtils.isNotEmpty(actionPids)) {
                params.add("pids=" + StringUtils.join(actionPids, ","));
            }
            url += "?" + StringUtils.join(params, "&");
        }
        put("patch owner id", url, null);
    }
}
