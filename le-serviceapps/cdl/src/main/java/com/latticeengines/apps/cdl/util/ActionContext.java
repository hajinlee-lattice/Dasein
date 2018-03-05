package com.latticeengines.apps.cdl.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.pls.Action;

public class ActionContext {
    private static Logger log = LoggerFactory.getLogger(ActionContext.class);

    private static ThreadLocal<Action> action = new ThreadLocal<Action>();

    public static void setAction(Action action) {
        ActionContext.action.set(action);
        log.info(String.format("Action %s set", action));
    }

    public static Action getAction() {
        return ActionContext.action.get();
    }

}
