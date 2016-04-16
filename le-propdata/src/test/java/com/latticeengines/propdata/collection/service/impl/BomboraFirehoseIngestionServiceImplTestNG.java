package com.latticeengines.propdata.collection.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraFirehose;
import com.latticeengines.propdata.engine.transform.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transform.configuration.impl.BomboraFirehoseConfiguration;
import com.latticeengines.propdata.engine.transform.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transform.service.TransformationService;
import com.latticeengines.propdata.engine.transform.service.impl.BomboraFirehoseIngestionService;

@Component
public class BomboraFirehoseIngestionServiceImplTestNG extends FirehoseTransformationServiceImplTestNGBase {

    @Autowired
    BomboraFirehoseIngestionService refreshService;

    @Autowired
    BomboraFirehose source;

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    TransformationProgressEntityMgr progressEntityMgr;

    @Override
    TransformationService getTransformationService() {
        return refreshService;
    }

    @Override
    TransformationProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    TransformationConfiguration createTransformationConfiguration() {
        BomboraFirehoseConfiguration conf = new BomboraFirehoseConfiguration();
        conf.setSourceName(source.getSourceName());
        Map<String, String> confMap = new HashMap<>();
        conf.setSourceConfigurations(confMap);
        conf.setInputFirehoseVersion(baseSourceVersion);
        conf.setVersion(baseSourceVersion);
        return conf;
    }
}
