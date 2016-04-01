package com.latticeengines.propdata.core.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.Source;

@Component("sourceService")
public class SourceServiceImpl implements SourceService {

    @Autowired
    private List<Source> sourceList;

    private Map<String, Source> sourceMap;

    @PostConstruct
    private void postConstruct() {
        sourceMap = new HashMap<>();
        for (Source source: sourceList) {
            sourceMap.put(source.getSourceName(), source);
        }
    }

    @Override
    public Source findBySourceName(String sourceName) {
        return sourceMap.get(sourceName);
    }

    @Override
    public List<Source> getSources() {
        return sourceList;
    }

}
