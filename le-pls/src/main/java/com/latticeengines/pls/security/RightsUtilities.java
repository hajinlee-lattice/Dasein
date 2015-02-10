package com.latticeengines.pls.security;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.security.EntityAccessRightsData;

public class RightsUtilities {

    public static Map<String, EntityAccessRightsData> translateRights(List<String> accessRights) {
        Map<String, EntityAccessRightsData> availableRights = new HashMap<>();

        for (String right : accessRights) {
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
            }
        }

        return availableRights;
    }
}
