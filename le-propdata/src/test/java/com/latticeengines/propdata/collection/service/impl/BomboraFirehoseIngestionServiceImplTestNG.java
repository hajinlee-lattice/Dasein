package com.latticeengines.propdata.collection.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraFirehose;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.engine.transformation.service.impl.BomboraFirehoseIngestionService;

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
        List<String> versionsToProcess = new ArrayList<>();
        versionsToProcess.add(baseSourceVersion);
        TransformationConfiguration conf = refreshService.createTransformationConfiguration(versionsToProcess);
        return conf;
    }
}
