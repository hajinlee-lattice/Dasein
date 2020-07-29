package com.latticeengines.domain.exposed.exception;

import java.util.Collections;
import java.util.Map;

public final class UIActionUtils {

    protected UIActionUtils() {
        throw new UnsupportedOperationException();
    }

    private static String title = "Action failed";

    // use UIActionCode.
    @Deprecated
    public static UIAction generateUIAction(String title, View view, Status status, String message) {
        UIAction uiAction;
        uiAction = new UIAction();
        uiAction.setTitle(title);
        uiAction.setView(view);
        uiAction.setStatus(status);
        uiAction.setMessage(message);
        return uiAction;
    }

    public static UIAction generateUIError(String title, View view, UIActionCode code) {
        return generateUIAction(title, view, Status.Error, code);
    }

    public static UIAction generateUIAction(String title, View view, Status status, UIActionCode code) {
        return generateUIAction(title, view, status, code, Collections.emptyMap());
    }

    public static UIAction generateUIAction(String title, View view, Status status, UIActionCode code, Map<String, Object> params) {
        UIAction uiAction;
        uiAction = new UIAction(code, params);
        uiAction.setTitle(title);
        uiAction.setView(view);
        uiAction.setStatus(status);
        return uiAction;
    }

    // non-exception
    public static UIAction generateUINotification(String title, View view, Status status, String message) {
        if (Status.Error.equals(status)) {
            throw new IllegalArgumentException("Please use generateUIError for exceptions!");
        }
        UIAction uiAction;
        uiAction = new UIAction();
        uiAction.setTitle(title);
        uiAction.setView(view);
        uiAction.setStatus(status);
        return uiAction;
    }

    public static UIActionException handleException(Exception ex) {
        UIAction uiAction;
        UIActionCode uiActionCode;
        Map<String, Object> params = null;
        if (ex instanceof UIActionException) {
            return (UIActionException) ex;
        } else if (ex instanceof LedpException) {
            LedpException ledpException = (LedpException) ex;
            uiActionCode = UIActionCode.fromLedpCode(ledpException.getCode());
            params = ledpException.getParamsMap();
        } else {
            uiActionCode = UIActionCode.SYSTEM_000;
        }
        uiAction = generateUIAction(title, View.Banner, Status.Error, uiActionCode, params);
        return UIActionException.fromAction(uiAction);
    }
}
