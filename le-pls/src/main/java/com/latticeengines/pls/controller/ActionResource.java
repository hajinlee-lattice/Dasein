package com.latticeengines.pls.controller;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.impl.GraphDependencyToUIActionUtil;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "action resource", description = "REST resource for action")
@RestController
@RequestMapping("/actions")
public class ActionResource {

    private static final Logger log = LoggerFactory.getLogger(ActionResource.class);

    @Inject
    private GraphDependencyToUIActionUtil graphDependencyToUIActionUtil;

    @Inject
    private ActionService actionService;

    private static final String CANCELSUCCESS_MSG = "<p>Cancel this action success.</p>";

    @RequestMapping(value = "/cancel", method = RequestMethod.POST)
    @ApiOperation(value = "cancel action")
    public Map<String, UIAction> cancelAction(@RequestParam(value = "actionPid") Long actionPid) {
        try {
            Action action = actionService.cancel(actionPid);
            log.info("action status is :" + action.getActionStatus().toString());
            if (action.getActionStatus() != ActionStatus.CANCELED) {
                throw new RuntimeException("Cannot cancel this action!");
            }
            UIAction uiAction = graphDependencyToUIActionUtil.generateUIAction("", View.Notice, Status.Success,
                    CANCELSUCCESS_MSG);
            return ImmutableMap.of(UIAction.class.getSimpleName(), uiAction);
        } catch (RuntimeException e) {
            log.error(String.format("Failed to cancel action: %s", e.getMessage()));
            throw new LedpException(LedpCode.LEDP_18211, new String[]{e.getMessage()});
        }
    }
}
