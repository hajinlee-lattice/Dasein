package com.latticeengines.api.controller;

import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.latticeengines.domain.exposed.api.Status;

@Controller
public class StatusResource {

    @RequestMapping(value = "/status/{op}/{left}/{right}", method = RequestMethod.GET)
    @ResponseBody
    public Status calculate(@PathVariable("op") String op, @PathVariable("left") Integer left,
            @PathVariable("right") Integer right) {
        Assert.notNull(op);
        Assert.notNull(left);
        Assert.notNull(right);
        Status result = new Status();
        result.setOperation(op);
        result.setLeft(left);
        result.setRight(right);
        return doCalc(result);
    }

    @RequestMapping(value = "/status", method = RequestMethod.POST)
    @ResponseBody
    public Status calculate(@RequestBody Status calc) {
        Assert.notNull(calc);
        Assert.notNull(calc.getOperation());
        Assert.notNull(calc.getLeft());
        Assert.notNull(calc.getRight());
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