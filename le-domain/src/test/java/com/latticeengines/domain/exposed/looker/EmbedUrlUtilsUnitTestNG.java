package com.latticeengines.domain.exposed.looker;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.validator.routines.UrlValidator;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class EmbedUrlUtilsUnitTestNG {

    private static final String[] OUTPUT_QUERY_PARAMETERS = new String[] { //
            "nonce", "time", "session_length", "external_user_id", "permissions", "models", //
            "access_filters", "first_name", "last_name", "signature", "group_ids", "external_group_id", //
            "user_attributes", "force_logout_login" //
    };

    @Test(groups = "unit")
    private void testCreateEmbedUrl() throws Exception {
        EmbedUrlData data = testData();

        // sign url
        String url = EmbedUrlUtils.signEmbedDashboardUrl(data);

        // assertions
        Assert.assertNotNull(url);
        URI uri = new URI(url);
        Assert.assertTrue(isURLValid(url), String.format("Signed URL %s is not valid", url));
        Assert.assertEquals(uri.getScheme(), "https");
        Assert.assertEquals(uri.getHost(), data.getHost());
        Assert.assertTrue(containsQueryParameter(url, OUTPUT_QUERY_PARAMETERS));

        System.out.println(url);
    }

    private EmbedUrlData testData() {
        Map<String, Object> userAttrs = new HashMap<>();
        userAttrs.put("target_account_list_table", "atlas_qa_performance_b3_account_list_data_v2");
        userAttrs.put("web_visit_data_table", "atlas_qa_performance_b3_ssvi_data_v2");

        EmbedUrlData data = new EmbedUrlData();
        data.setHost("dnblattice.cloud.looker.com");
        data.setSecret("fake_secret");
        data.setExternalUserId("stephen-embed-test-user");
        data.setFirstName("Stephen");
        data.setLastName("Lin");
        data.setGroupIds(Collections.singletonList(3));
        data.setPermissions(Arrays.asList("see_drill_overlay", "see_lookml_dashboards", "access_data"));
        data.setModels(Collections.singletonList("ssvi"));
        data.setSessionLength(30 * 60L);
        data.setEmbedUrl(EmbedUrlUtils.embedUrl("ssvi", "overview"));
        data.setForceLogoutLogin(true);
        data.setUserAttributes(userAttrs);
        return data;
    }

    private boolean isURLValid(@NotNull String url) {
        return UrlValidator.getInstance().isValid(url);
    }

    private boolean containsQueryParameter(String url, String[] queryParam) {
        UriComponents components = UriComponentsBuilder.fromUriString(url).build();
        MultiValueMap<String, String> queryParams = components.getQueryParams();
        return Arrays.stream(queryParam).allMatch(queryParams::containsKey);
    }
}
