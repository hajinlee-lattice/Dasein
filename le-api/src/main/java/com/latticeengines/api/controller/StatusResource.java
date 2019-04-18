package com.latticeengines.api.controller;

import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.Status;
import com.wordnik.swagger.annotations.Api;

@Api(value = "status", description = "Status of the REST API endpoint")
@RestController
public class StatusResource {

    @RequestMapping(value = "/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
    public Status calculate(@PathVariable("op") String op, @PathVariable("left") Integer left,
            @PathVariable("right") Integer right) {
        Assert.notNull(op, "PathVariable op cannot be null");
        Assert.notNull(left, "PathVariable left cannot be null");
        Assert.notNull(right, "PathVariable right cannot be null");
        Status result = new Status();
        result.setOperation(op);
        result.setLeft(left);
        result.setRight(right);
        return doCalc(result);
    }

    @RequestMapping(value = "/post", method = RequestMethod.POST)
    @ResponseBody
    public Status calculate(@RequestBody Status calc) {
        Assert.notNull(calc, "RequestBody status cannot be null");
        Assert.notNull(calc.getOperation(), "RequestBody status.operation cannot be null");
        Assert.notNull(calc.getLeft(), "RequestBody status.left cannot be null");
        Assert.notNull(calc.getRight(), "RequestBody status.right cannot be null");
        return doCalc(calc);
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
