package com.latticeengines.apps.cdl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.Action;

public final class ActionContext {

    protected ActionContext() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(ActionContext.class);

    private static ThreadLocal<Action> action = new ThreadLocal<Action>();

    public static void setAction(Action actionVal) {
        action.set(actionVal);
        log.info(String.format("Action %s set", actionVal));
    }

    public static Action getAction() {
        return action.get();
    }

    public static void remove() {
        action.remove();
    }

}
