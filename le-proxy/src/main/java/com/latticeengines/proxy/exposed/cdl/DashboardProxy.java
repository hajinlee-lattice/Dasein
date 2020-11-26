package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.dashboard.Dashboard;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilter;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("dashboardProxy")
public class DashboardProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    public DashboardProxy() {
        super("cdl");
    }

    public DashboardProxy(String hostPort) {
        super(hostPort, "cdl");
    }

    public Dashboard createDashboard(String customerSpace, Dashboard dashboard) {
        String url = constructUrl("/customerspaces/{customerSpace}/vireports/dashboard", shortenCustomerSpace(customerSpace));
        return post("create dashboard", url, dashboard, Dashboard.class);
    }

    public void createDashboardList(String customerSpace, List<Dashboard> dashboardList) {
        String url = constructUrl("/customerspaces/{customerSpace}/vireports/dashboard/createList", shortenCustomerSpace(customerSpace));
        post("create dashboard list", url, dashboardList);
    }

    public List<Dashboard> getDashboards(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/vireports/dashboard", shortenCustomerSpace(customerSpace));
        List<?> dashboardList = get("get all dashboard", url, List.class);
        return JsonUtils.convertList(dashboardList, Dashboard.class);
    }

    public List<DashboardFilter> getDashboardFilters(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/vireports/dashboard/filters", shortenCustomerSpace(customerSpace));
        List<?> dashboardFilterList = get("get all dashboardfilter", url, List.class);
        return JsonUtils.convertList(dashboardFilterList, DashboardFilter.class);
    }
}
