package com.latticeengines.domain.exposed.exception;

import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public class UIActionException extends RuntimeException {

    private static final long serialVersionUID = 5890780873823715440L;
    private LedpCode code;
    private UIAction uiAction;

    public UIActionException(UIAction uiAction, LedpCode code) {
        this.setCode(code);
        this.uiAction = uiAction;
    }

    public UIAction getUIAction() {
        return this.uiAction;
    }

    public void setUIAction(UIAction uiAction) {
        this.uiAction = uiAction;
    }

    public LedpCode getCode() {
        return code;
    }

    public void setCode(LedpCode code) {
        this.code = code;
    }

}
