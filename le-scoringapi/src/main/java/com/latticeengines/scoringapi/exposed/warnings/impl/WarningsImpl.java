package com.latticeengines.scoringapi.exposed.warnings.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.scoringapi.exposed.warnings.Warning;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;

@Component("warnings")
public class WarningsImpl implements Warnings {

    private static final String WARNINGS_KEY = "com.latticeengines.warnings";

    @Override
    public void addWarning(Warning codeMessage) {
        List<Warning> warnings = getOrCreate();
        warnings.add(codeMessage);
    }

    @Override
    public List<Warning> getWarnings() {
        return getOrCreate();
    }

    @Override
    public boolean hasWarnings() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(WARNINGS_KEY);
        return attribute != null;
    }

    @SuppressWarnings("unchecked")
    private List<Warning> getOrCreate() {
        List<Warning> warnings = null;
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(WARNINGS_KEY);
        if (attribute == null) {
            warnings = new ArrayList<>();
            attributes.getRequest().setAttribute(WARNINGS_KEY, warnings);
        } else {
            warnings = (List<Warning>) attribute;
        }
        return warnings;
    }


}
