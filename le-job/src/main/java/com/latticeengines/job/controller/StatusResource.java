package com.latticeengines.job.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.Status;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;


@Api(value = "internal", description = "REST resource for status check")
@RestController
@RequestMapping(value = "/status")
public class StatusResource {

    @RequestMapping(value = "/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Status check for this endpoint")
    public Status calculate(@PathVariable("op") String op, @PathVariable("left") Integer left,
            @PathVariable("right") Integer right) {
        Status result = new Status();
        result.setOperation(op);
        result.setLeft(left);
        result.setRight(right);
        return doCalc(result);
    }

    private Status doCalc(Status c) {
        String op = c.getOperation();
        int left = c.getLeft();
        int right = c.getRight();
        if (op.equalsIgnoreCase("subtract")) {
            c.setResult(left - right);
        } else if (op.equalsIgnoreCase("multiply")) {
            c.setResult(left * right);
        } else if (op.equalsIgnoreCase("divide")) {
            c.setResult(left / right);
        } else {
            c.setResult(left + right);
        }
        return c;
    }

}
