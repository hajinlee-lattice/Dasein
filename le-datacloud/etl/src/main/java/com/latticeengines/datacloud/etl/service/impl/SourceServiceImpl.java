package com.latticeengines.datacloud.etl.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.service.SourceService;

@Component("sourceService")
public class SourceServiceImpl implements SourceService {

    @Autowired
    private List<Source> sourceList;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

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
        Source source =  sourceMap.get(sourceName);
        if (source == null) {
            GeneralSource generalSource = new GeneralSource();
            generalSource.setSourceName(sourceName);
            if (hdfsSourceEntityMgr.checkSourceExist(generalSource)) {
                source = (Source)generalSource;
                sourceMap.put(sourceName, source);
            }
        }
        return source;
    }

    @Override
    public List<Source> getSources() {
        return sourceList;
    }

    public Source createSource(String sourceName) {
        GeneralSource source = new GeneralSource();
        source.setSourceName(sourceName);
        hdfsSourceEntityMgr.initiateSource(source);
        sourceMap.put(sourceName, source);
        return (Source)source;
    }

    public Source findOrCreateSource(String sourceName) {
        Source source = findBySourceName(sourceName);
        if (source == null) {
            source = createSource(sourceName);
        }
        return source;
    }

    public boolean deleteSource (Source source) {

        if (!(source instanceof GeneralSource)) {
            return false;
        }
        hdfsSourceEntityMgr.deleteSource(source);
        sourceMap.remove(source.getSourceName());
        return true;
   }

}
