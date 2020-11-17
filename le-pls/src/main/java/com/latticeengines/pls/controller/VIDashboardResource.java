package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardFilterValue;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "VI dashboard resource", description = "REST resource for action")
@RestController
@RequestMapping("/vidashboards")
public class VIDashboardResource {

    private static final Logger log = LoggerFactory.getLogger(VIDashboardResource.class);

    @GetMapping("/dashboards")
    @ResponseBody
    @ApiOperation("get all related dashboards")
    public Map<String, String> getDashboardList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return getMockDashboards();
    }

    @GetMapping("/dashboardFilters")
    @ResponseBody
    @ApiOperation("get all related dashboards")
    public Map<String, List<DashboardFilterValue>> getDashboardFilterList() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return getMockFilterValue();
    }

    private Map<String, List<DashboardFilterValue>> getMockFilterValue() {
        Map<String, List<DashboardFilterValue>> mockFilterMap = new HashMap<>();
        List<DashboardFilterValue> values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("Last 2 Weeks").withValue("2w").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("Last 12 Months").withValue("12m").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("Last 15 Months").withValue("15m").build());
        mockFilterMap.put("TIME_FILTER", values);
        values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("1-10").withValue("1-10").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("11-50").withValue("11-50").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("51-100").withValue("51-100").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName(">10,000").withValue(">10,000").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("101-200").withValue("101-200").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("201-500").withValue("201-500").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("501-1000").withValue("501-1000").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("1001-2500").withValue("1001-2500").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("2501-5000").withValue("2501-5000").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("5001-10,000").withValue("5001-10,000").build());
        mockFilterMap.put("EMPLOYEE_FILTER", values);
        values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("EATING PLACES").withValue("EATING PLACES").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("BUSINESS SERVICES, NEC").withValue("BUSINESS SERVICES, NEC").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("LEGAL SERVICES").withValue("LEGAL SERVICES").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("RELIGIOUS ORGANIZATIONS").withValue("RELIGIOUS ORGANIZATIONS").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("REAL ESTATE AGENTS AND MANAGERS").withValue("REAL ESTATE AGENTS AND MANAGERS").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("OFFICES AND CLINICS OF MEDICAL DOCTORS").withValue("OFFICES AND CLINICS OF MEDICAL DOCTORS").build());
        mockFilterMap.put("INDUSTRY_FILTER", values);
        values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("CALIFORNIA").withValue("CALIFORNIA").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("FLORIDA").withValue("FLORIDA").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("TEXAS").withValue("TEXAS").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("ILLINOIS").withValue("ILLINOIS").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("PENNSYLVANIA").withValue("PENNSYLVANIA").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("OHIO").withValue("OHIO").build());
        mockFilterMap.put("LOCATION_FILTER", values);
        values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("0-1M").withValue("0-1M").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("1-10M").withValue("1-10M").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("11-50M").withValue("11-50M").build());
        mockFilterMap.put("REVENUE_FILTER", values);
        values = new ArrayList<>();
        values.add(new DashboardFilterValue.Builder().withDisplayName("All Contents").withValue("All Contents").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("Team Page").withValue("Team Page").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("News").withValue("News").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("About Page").withValue("About Page").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("Login Page").withValue("Login Page").build());
        values.add(new DashboardFilterValue.Builder().withDisplayName("Home Page").withValue("Home Page").build());
        mockFilterMap.put("PAGE_FILTER", values);
        return mockFilterMap;
    }

    private Map<String, String> getMockDashboards() {
        Map<String, String> dashboards = new HashMap<>();
        dashboards.put("employee", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws" +
                ".com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/5d58a290-2339-11eb-bcb2-2f4292783cad?_g=(filters:!()," +
                "refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:''," +
                "filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:b42b0450-1e7f-11eb-a43a-5d33a24fd950," +
                "key:LE_EMPLOYEE_RANGE.keyword,negate:!f,params:(query:'<EMPLOYEE_FILTER>'),type:phrase),query:(match_phrase:" +
                "(LE_EMPLOYEE_RANGE.keyword:'<EMPLOYEE_FILTER>')))),fullScreenMode:!f,options:(hidePanelTitles:!f," +
                "useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_company_employee,viewMode:view)");
        dashboards.put("industry", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws.com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/7eac1ee0-232a-11eb-bcb2-2f4292783cad?_g=" +
                "(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:''," +
                "filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f," +
                "index:b42b0450-1e7f-11eb-a43a-5d33a24fd950,key:LE_INDUSTRY.keyword,negate:!f,params:" +
                "(query:'<INDUSTRY_FILTER>'),type:phrase),query:(match_phrase:(LE_INDUSTRY.keyword:'<INDUSTRY_FILTER>')" +
                "))),fullScreenMode:!f,options:(hidePanelTitles:!f,useMargins:!t),query:(language:kuery,query:'')," +
                "timeRestore:!f,title:ssvi_poc_m41_company_industry,viewMode:view)");
        dashboards.put("location", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws.com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/86e9e880-2334-11eb-a43a-5d33a24fd950?_g=" +
                "(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:''," +
                "filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f," +
                "index:b42b0450-1e7f-11eb-a43a-5d33a24fd950,key:LDC_State.keyword,negate:!f,params:(query:<LOCATION_FILTER>)" +
                ",type:phrase),query:(match_phrase:(LDC_State.keyword:<LOCATION_FILTER>)))),fullScreenMode:!f," +
                "options:(hidePanelTitles:!f,useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_company_location,viewMode:view)");
        dashboards.put("revenue", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws" +
                ".com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/a2f89ba0-2336-11eb-a43a-5d33a24fd950?_g=" +
                "(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:''," +
                "filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f," +
                "index:b42b0450-1e7f-11eb-a43a-5d33a24fd950,key:LE_REVENUE_RANGE.keyword,negate:!f,params:" +
                "(query:'<REVENUE_FILTER>'),type:phrase),query:(match_phrase:(LE_REVENUE_RANGE.keyword:'<REVENUE_FILTER>'))))," +
                "fullScreenMode:!f,options:(hidePanelTitles:!f,useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_company_revenue,viewMode:view)");
        dashboards.put("page", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws" +
                ".com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/62ff2e40-233d-11eb-bcb2-2f4292783cad?_g=(filters:!()," +
                "refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:'',filters:!(" +
                "('$state':(store:appState),meta:(alias:!n,disabled:!f,index:b42b0450-1e7f-11eb-a43a-5d33a24fd950," +
                "key:UrlCategories.keyword,negate:!f,params:(query:'<PAGE_FILTER>'),type:phrase),query:" +
                "(match_phrase:(UrlCategories.keyword:'<PAGE_FILTER>')))),fullScreenMode:!f,options:" +
                "(hidePanelTitles:!f,useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_page_by_group,viewMode:view)");
        dashboards.put("pagegroup", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws.com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/b346c1d0-2340-11eb-a43a-5d33a24fd950?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:'',filters:!(),fullScreenMode:!f,options:(hidePanelTitles:!f,useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_page_by_page,viewMode:view)");
        dashboards.put("overview", "https://internal-kibana-internal-1262013531.us-east-1.elb.amazonaws.com/_plugin/kibana/app/kibana?global_auth_token=<GLOBAL_AUTH_TOKEN>#/dashboard/cce9ae10-22df-11eb-bcb2-2f4292783cad?_g=(filters:!(),refreshInterval:(pause:!t,value:0),time:(from:now-<TIME_FILTER>,to:now))&_a=(description:'',filters:!(),fullScreenMode:!f,options:(hidePanelTitles:!t,useMargins:!t),query:(language:kuery,query:''),timeRestore:!f,title:ssvi_poc_m41_v1_overview,viewMode:view)");
        return dashboards;
    }
}
