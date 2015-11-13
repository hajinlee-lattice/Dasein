package com.latticeengines.admin.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.domain.exposed.admin.SelectableConfigurationField;

public class InternalResourceTestNG extends AdminFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testPathDefaultConfigThroughREST() throws Exception {
        String url = hostPort + "/admin/internal/services/options?component=VisiDBDL";

        // ==================================================
        // restore
        // ==================================================
        SelectableConfigurationField patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Arrays.asList("bodcdevvint187", "bodcdevvint207"));
        patch.setDefaultOption("bodcdevvint187");

        boolean success = patchByHTTPPut(url, patch);
        Assert.assertTrue(success);

        // ==================================================
        // invalid default option
        // ==================================================
        patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Arrays.asList("bodcdevvint187", "bodcdevvint207"));
        patch.setDefaultOption("nope");

        boolean gotException = false;
        try {
            patchByHTTPPut(url, patch);
        } catch (Exception e) {
            gotException = true;
        }
        Assert.assertTrue(gotException, "should got exception because of invalid default option.");

        // ==================================================
        // make exsiting default invalid
        // ==================================================
        patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Collections.singletonList("bodcdevvint207"));

        gotException = false;
        try {
            patchByHTTPPut(url, patch);
        } catch (Exception e) {
            gotException = true;
        }
        Assert.assertTrue(gotException, "should got exception because of invalid default option.");

        // ==================================================
        // make exsiting default invalid
        // ==================================================
        patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Arrays.asList("bodcdevvint187", "bodcdevvint207", "bodcdevvint217"));

        Assert.assertTrue(patchByHTTPPut(url, patch));

        // ==================================================
        // valid patch
        // ==================================================
        patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Arrays.asList("option1", "option2"));
        patch.setDefaultOption("option1");

        Assert.assertTrue(patchByHTTPPut(url, patch));

        // ==================================================
        // restore
        // ==================================================
        patch = new SelectableConfigurationField();
        patch.setNode("/VisiDB/ServerName");
        patch.setOptions(Arrays.asList("bodcdevvint187", "bodcdevvint207"));
        patch.setDefaultOption("bodcdevvint187");

        Assert.assertTrue(patchByHTTPPut(url, patch));
    }

    private Boolean patchByHTTPPut(String url, SelectableConfigurationField patch) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String payload = mapper.writeValueAsString(patch);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<String> requestEntity = new HttpEntity<>(payload, headers);
        ResponseEntity<Boolean> response = magicRestTemplate
                .exchange(url, HttpMethod.PUT, requestEntity, Boolean.class);
        return response.getBody();
    }

}
