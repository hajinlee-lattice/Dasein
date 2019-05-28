package com.latticeengines.common.exposed.util;

public class EmailNotificationValidateUtils {

    public static boolean validNotificationStateForS3Import(String result, boolean emailInfo,
                                                            int notificationState) {
        if (result.equalsIgnoreCase("Failed") && (notificationState & 1) == 1) {
            return true;
        }
        if (result.equalsIgnoreCase("In_Progress")) {
            if (!emailInfo && (notificationState & 4) == 4) {
                return true;
            }
            return emailInfo && (notificationState & 2) == 2;
        }
        if (result.equalsIgnoreCase("Success") && (notificationState & 4) == 4) {
            return true;
        }
        return false;
    }
}
