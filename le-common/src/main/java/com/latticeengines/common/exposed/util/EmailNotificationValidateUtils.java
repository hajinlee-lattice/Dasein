package com.latticeengines.common.exposed.util;

public class EmailNotificationValidateUtils {

    public static boolean validNotificationStateForS3Import(String result, boolean emailInfo,
                                                            String notificationState) {
        if (result.equalsIgnoreCase("Failed") && notificationState.equalsIgnoreCase("ERROR")) {
            return true;
        }
        if (result.equalsIgnoreCase("In_Progress")) {
            if (!emailInfo && notificationState.equalsIgnoreCase("INFO")) {
                return true;
            }
            return emailInfo && notificationState.equalsIgnoreCase("WARNING");
        }
        if (result.equalsIgnoreCase("Success") && notificationState.equalsIgnoreCase("INFO")) {
            return true;
        }
        return false;
    }
}
