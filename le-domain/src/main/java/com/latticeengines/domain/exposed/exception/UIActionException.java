package com.latticeengines.domain.exposed.exception;

public class UIActionException extends RuntimeException {

    private static final long serialVersionUID = 5890780873823715440L;
    private final UIAction uiAction;

    public static UIActionException fromAction(UIAction uiAction) {
        UIActionException exception = null;
        UIActionCode code = uiAction.getCode();
        if (code != null) {
            UIActionCode.Level level = code.getLevel();
            if (UIActionCode.Level.User.equals(level)) {
                exception = new UserError(uiAction);
            }
        }
        if (exception == null) {
            exception = new SystemError(uiAction);
        }
        return exception;
    }

    // register error template in UIActionCode
    // then use UIActionUtils to create UIAction from UIActionCode
    @Deprecated
    public UIActionException(UIAction uiAction, LedpCode code) {
        this.uiAction = uiAction;
    }

    private UIActionException(UIAction uiAction) {
        this.uiAction = uiAction;
    }

    public UIAction getUIAction() {
        return this.uiAction;
    }

    public static class UserError extends UIActionException {
        private UserError(UIAction uiAction) {
            super(uiAction);
        }
    }

    public static class SystemError extends UIActionException {
        private SystemError(UIAction uiAction) {
            super(uiAction);
        }
    }

}
