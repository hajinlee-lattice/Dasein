package com.latticeengines.security.exposed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.security.EntityAccessRightsData;

public final class RightsUtilities {

    protected RightsUtilities() {
        throw new UnsupportedOperationException();
    }

    public static Map<String, EntityAccessRightsData> translateRights(List<String> accessRights) {
        Map<String, EntityAccessRightsData> availableRights = new HashMap<>();

        for (String right : accessRights) {
            if (GrantedRight.getGrantedRight(right) == null) break;
            String[] rightPair = right.split("_", 2);
            EntityAccessRightsData rightsDocument = availableRights.get(rightPair[1]);
            if (rightsDocument == null) {
                rightsDocument = new EntityAccessRightsData();
                availableRights.put(rightPair[1], rightsDocument);
            }

            switch (rightPair[0].toLowerCase()) {
                case "view":
                    rightsDocument.setMayView(true);
                    break;
                case "edit":
                    rightsDocument.setMayEdit(true);
                    break;
                case "execute":
                    rightsDocument.setMayExecute(true);
                    break;
                case "create":
                    rightsDocument.setMayCreate(true);
                    break;
                default:
            }
        }

        return availableRights;
    }

    public static List<String> translateRights(Map<String, EntityAccessRightsData> rightsDataMap) {
        List<String> accessRights = new ArrayList<>();
        if (rightsDataMap != null) {
            for (Map.Entry<String, EntityAccessRightsData> entry : rightsDataMap.entrySet()) {
                String key = entry.getKey();
                EntityAccessRightsData rightsDocument = entry.getValue();
                if (rightsDocument.isMayView()) {
                    accessRights.add("View_" + key);
                }
                if (rightsDocument.isMayEdit()) {
                    accessRights.add("Edit_" + key);
                }
                if (rightsDocument.isMayExecute()) {
                    accessRights.add("Execute_" + key);
                }
                if (rightsDocument.isMayCreate()) {
                    accessRights.add("Create_" + key);
                }
            }
        }
        return accessRights;
    }
}
