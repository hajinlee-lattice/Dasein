package com.latticeengines.domain.exposed.exception;

import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public class UIActionException extends LedpException {

    private static final long serialVersionUID = 5890780873823715440L;

    private UIAction uiAction;

    public UIActionException(LedpCode code) {
        super(code);
    }

    public UIActionException(UIAction uiAction, LedpCode code) {
        super(code);
        this.uiAction = uiAction;
    }

    public UIActionException(UIAction uiAction, LedpCode code, Throwable t) {
        super(code, t);
        this.uiAction = uiAction;
    }

    public UIActionException(UIAction uiAction, LedpCode code, String msg) {
        super(code, msg, null);
        this.uiAction = uiAction;
    }

    public UIActionException(UIAction uiAction, LedpCode code, String msg, Throwable t) {
        super(code, msg, t);
        this.uiAction = uiAction;
    }

    public UIAction getUIAction() {
        return this.uiAction;
    }

    public void setUIAction(UIAction uiAction) {
        this.uiAction = uiAction;
    }

}
