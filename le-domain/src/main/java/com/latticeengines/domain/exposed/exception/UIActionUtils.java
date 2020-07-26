package com.latticeengines.domain.exposed.exception;

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
        UIAction uiAction;
        uiAction = new UIAction(code);
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
        LedpCode code;
        if (ex instanceof UIActionException) {
            return (UIActionException) ex;
        } else if (ex instanceof LedpException) {
            code = ((LedpException) ex).getCode();
        } else {
            code = LedpCode.LEDP_00002;
        }
        String removeStartingText = code.name() + ": ";
        String message = ex.getMessage();
        if (message.startsWith(removeStartingText)) {
            message = message.substring(removeStartingText.length());
        }
        uiAction = generateUIAction(title, View.Banner, Status.Error, message);
        return new UIActionException(uiAction, code);
    }
}
