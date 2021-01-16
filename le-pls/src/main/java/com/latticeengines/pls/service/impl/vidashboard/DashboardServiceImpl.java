package com.latticeengines.pls.service.impl.vidashboard;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;
import com.latticeengines.domain.exposed.looker.EmbedUrlData;
import com.latticeengines.domain.exposed.looker.EmbedUrlUtils;
import com.latticeengines.pls.service.vidashboard.DashboardService;

@Component("dashboardService")
public class DashboardServiceImpl implements DashboardService {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImpl.class);

    private static final List<String> SSVI_USER_PERMISSIONS = Arrays.asList("see_drill_overlay",
            "see_lookml_dashboards", "access_data");
    private static final List<String> SSVI_DASHBOARDS = Arrays.asList("overview", "accounts_visited", "page_analysis");
    private static final String USER_ATTR_WEB_VISIT_DATA_TABLE = "web_visit_data_table";
    private static final String USER_ATTR_TARGET_ACCOUNT_LIST_TABLE = "target_account_list_table";

    @Value("${pls.looker.host}")
    private String lookerHost;

    @Value("${pls.looker.secret.encrypted}")
    private String lookerEmbedSecret;

    @Value("${pls.looker.session.seconds:2400}")
    private Long lookerSessionLengthInSeconds;

    @Value("${pls.looker.ssvi.model}")
    private String ssviLookerModelName;

    @Value("${pls.looker.ssvi.usergroup.id}")
    private Integer ssviUserGroupId;

    @Override
    public DashboardResponse getDashboardList(String customerSpace) {
        DashboardResponse res = new DashboardResponse();
        res.setDashboardUrls(getDashboardMap(customerSpace));
        return res;
    }

    /*-
     * FIXME change to real implementation, currently return mock data to unblock UI
     */
    private Map<String, String> getDashboardMap(@NotNull String customerSpace) {
        String tenant = CustomerSpace.shortenCustomerSpace(customerSpace);
        return SSVI_DASHBOARDS.stream().map(dashboard -> {
            EmbedUrlData data = new EmbedUrlData();
            data.setHost(lookerHost);
            data.setSecret(lookerEmbedSecret);
            data.setExternalUserId(tenant);
            data.setFirstName("SSVI");
            data.setLastName("User");
            data.setGroupIds(Collections.singletonList(ssviUserGroupId));
            data.setPermissions(SSVI_USER_PERMISSIONS);
            data.setModels(Collections.singletonList(ssviLookerModelName));
            data.setSessionLength(lookerSessionLengthInSeconds);
            data.setEmbedUrl(EmbedUrlUtils.embedUrl(ssviLookerModelName, dashboard));
            data.setForceLogoutLogin(true);
            data.setUserAttributes(mockUserAttributes());
            return Pair.of(dashboard, EmbedUrlUtils.signEmbedDashboardUrl(data));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private Map<String, Object> mockUserAttributes() {
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put(USER_ATTR_WEB_VISIT_DATA_TABLE, "atlas_qa_performance_b3_ssvi_data_v2");
        userAttrs.put(USER_ATTR_TARGET_ACCOUNT_LIST_TABLE, "atlas_qa_performance_b3_account_list_data_v2");
        return userAttrs;
    }

}
