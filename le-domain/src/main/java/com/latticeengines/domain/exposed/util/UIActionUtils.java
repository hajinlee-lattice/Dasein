package com.latticeengines.domain.exposed.util;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;

public final class UIActionUtils {

    protected UIActionUtils() {
        throw new UnsupportedOperationException();
    }

    private static String title = "Action failed";

    public static UIAction generateUIAction(String title, View view, Status status, String message) {
        UIAction uiAction;
        uiAction = new UIAction();
        uiAction.setTitle(title);
        uiAction.setView(view);
        uiAction.setStatus(status);
        uiAction.setMessage(message);
        return uiAction;
    }

    public static UIActionException handleExceptionForCreateOrUpdate(Exception ex) {
        UIAction uiAction;
        LedpCode code;
        if (ex instanceof LedpException) {
            code = ((LedpException) ex).getCode();
        } else {
            code = LedpCode.LEDP_00002;
        }
        uiAction = generateUIAction(title, View.Banner, Status.Error, ex.getMessage());
        return new UIActionException(uiAction, code);
    }
}
