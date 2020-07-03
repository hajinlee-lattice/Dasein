package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.auth.TeamEntities;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.TeamProxy;

@Component("teamProxy")
public class TeamProxyImpl extends MicroserviceRestApiProxy implements TeamProxy {

    protected TeamProxyImpl() {
        super("cdl");
    }

    @Override
    public TeamEntities getTeamEntities(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/teams/team-entities", //
                shortenCustomerSpace(customerSpace));
        return get("find atlas export by id", url, TeamEntities.class);
    }
}
