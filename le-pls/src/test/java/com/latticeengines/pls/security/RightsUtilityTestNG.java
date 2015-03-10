package com.latticeengines.pls.security;

import com.latticeengines.domain.exposed.security.EntityAccessRightsData;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

public class RightsUtilityTestNG extends PlsFunctionalTestNGBase {

    @Test(groups = {"functional", "deployment"})
    public void translate() {
        // from list of strings to map or rights data
        List<String> rights = Arrays.asList("View_PLS_Models", "View_PLS_Users", "Edit_PLS_Users", "View_PLS_Reporting", "Edit_PLS_Configuration");
        Map<String, EntityAccessRightsData> rightsDataMap = RightsUtilities.translateRights(rights);
        for (Map.Entry<String, EntityAccessRightsData> entry: rightsDataMap.entrySet()) {
            if (entry.getKey().equals("PLS_Models")) {
                assertTrue(entry.getValue().isMayView());
                assertFalse(entry.getValue().isMayEdit());
                assertFalse(entry.getValue().isMayCreate());
                assertFalse(entry.getValue().isMayExecute());
            } else if (entry.getKey().equals("PLS_Users")) {
                assertTrue(entry.getValue().isMayView());
                assertTrue(entry.getValue().isMayEdit());
                assertFalse(entry.getValue().isMayCreate());
                assertFalse(entry.getValue().isMayExecute());
            } else if (entry.getKey().equals("PLS_Configuration")) {
                assertFalse(entry.getValue().isMayView());
                assertTrue(entry.getValue().isMayEdit());
                assertFalse(entry.getValue().isMayCreate());
                assertFalse(entry.getValue().isMayExecute());
            } else if (entry.getKey().equals("PLS_Reporting")) {
                assertTrue(entry.getValue().isMayView());
                assertFalse(entry.getValue().isMayEdit());
                assertFalse(entry.getValue().isMayCreate());
                assertFalse(entry.getValue().isMayExecute());
            } else {
                assertTrue(false, "Unexpected rights group " + entry.getKey());
            }
        }

        List<String> decodedRights = RightsUtilities.translateRights(rightsDataMap);
        assertTrue(decodedRights.containsAll(rights));
    }
}
