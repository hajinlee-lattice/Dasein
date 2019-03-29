package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    public Action cancelAction(String customerSpace, Long actionPid) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions?actionPid={actionPid}", //
                shortenCustomerSpace(customerSpace), String.valueOf(actionPid));
        return get("cancel action", url, Action.class);
    }

    @Override
    public List<Action> getActions(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions/find", shortenCustomerSpace(customerSpace));
        Map<String, Object> actionParameter = new HashMap<>();
        actionParameter.put("pids", null);
        actionParameter.put("nullOwnerId", false);
        actionParameter.put("ownerId", null);
        List<?> list = post("get actions", url, actionParameter, List.class);
        return JsonUtils.convertList(list, Action.class);
    }

    @Override
    public List<Action> getActionsByOwnerId(String customerSpace, Long ownerId) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions/find", shortenCustomerSpace(customerSpace));
        Map<String, Object> actionParameter = new HashMap<>();
        actionParameter.put("pids", null);
        if (ownerId == null) {
            actionParameter.put("nullOwnerId", true);
            actionParameter.put("ownerId", null);
        } else {
            actionParameter.put("nullOwnerId", false);
            actionParameter.put("ownerId", ownerId);
        }
        List<?> list = post("get actions by owner", url, actionParameter, List.class);
        return JsonUtils.convertList(list, Action.class);
    }

    @Override
    public List<Action> getActionsByPids(String customerSpace, List<Long> actionPids) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions/find", shortenCustomerSpace(customerSpace));
        Map<String, Object> actionParameter = new HashMap<>();
        actionParameter.put("pids", actionPids);
        actionParameter.put("ownerId", null);
        actionParameter.put("nullOwnerId", false);
        List<?> list = post("get actions by pids", url, actionParameter, List.class);
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

    @Override
    public List<Action> getActionsByJobPid(String customerSpace, Long jobPid) {
        String url = constructUrl("/customerspaces/{customerSpace}/actions/actions?jobPid={jobPid}",
                shortenCustomerSpace(customerSpace), String.valueOf(jobPid));
        List<?> list = get("get Actions By JobPid", url, List.class);
        return JsonUtils.convertList(list, Action.class);
    }
}
