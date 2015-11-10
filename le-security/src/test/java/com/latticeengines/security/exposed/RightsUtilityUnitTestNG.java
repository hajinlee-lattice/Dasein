package com.latticeengines.security.exposed;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.EntityAccessRightsData;

public class RightsUtilityUnitTestNG {

    @Test(groups = "unit")
    public void translate() {
        // from list of strings to map or rights data
        List<String> rights = Arrays.asList("View_PLS_Models", "View_PLS_Users", "Edit_PLS_Users", "View_PLS_Reporting", "Edit_PLS_Configurations");
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
            } else if (entry.getKey().equals("PLS_Configurations")) {
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
