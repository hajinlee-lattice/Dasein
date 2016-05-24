package com.latticeengines.scoringapi.exposed.warnings.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.Warnings;

@Component("warnings")
public class WarningsImpl implements Warnings {

    private static final String WARNINGS_KEY = "com.latticeengines.scoringapi.warnings";

    @Override
    public void addWarning(Warning codeMessage) {
        List<Warning> warnings = getOrCreate(null);
        warnings.add(codeMessage);
    }

    @Override
    public void addWarning(String recordId, Warning codeMessage) {
        List<Warning> warnings = getOrCreate(recordId);
        warnings.add(codeMessage);
    }

    @Override
    public List<Warning> getWarnings() {
        return getOrCreate(null);
    }

    @Override
    public List<Warning> getWarnings(String recordId) {
        return getOrCreate(recordId);
    }

    @Override
    public boolean hasWarnings() {
        return hasWarnings(null);
    }

    @Override
    public boolean hasWarnings(String recordId) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        String key = getKey(recordId);
        Object attribute = attributes.getRequest().getAttribute(key);
        return attribute != null;
    }

    private String getKey(String recordId) {
        String key = WARNINGS_KEY;
        if (!StringUtils.isEmpty(recordId)) {
            key += "_" + recordId;
        }
        return key;
    }

    @SuppressWarnings("unchecked")
    private List<Warning> getOrCreate(String recordId) {
        List<Warning> warnings = null;
        String key = getKey(recordId);
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(key);
        if (attribute == null) {
            warnings = new ArrayList<>();
            attributes.getRequest().setAttribute(key, warnings);
        } else {
            warnings = (List<Warning>) attribute;
        }
        return warnings;
    }

}
