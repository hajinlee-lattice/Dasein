package com.latticeengines.propdata.collection.service.impl;

import java.util.HashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraDepivoted;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraDepivotConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;
import com.latticeengines.propdata.engine.transformation.service.impl.BomboraDepivotedService;

@Component
public class BomboraDepivotServiceImplTestNG extends FixedIntervalTransformationServiceTestNGBase {

    @Autowired
    BomboraDepivotedService refreshService;

    @Autowired
    BomboraDepivoted source;

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
        BomboraDepivotConfiguration conf = new BomboraDepivotConfiguration();
        conf.setSourceName(source.getSourceName());
        conf.setSourceConfigurations(new HashMap<String, String>());
        BomboraFirehoseInputSourceConfig bomboraFirehoseInputSourceConfig = new BomboraFirehoseInputSourceConfig();
        bomboraFirehoseInputSourceConfig.setVersion(baseSourceVersion);
        conf.setBomboraFirehoseInputSourceConfig(bomboraFirehoseInputSourceConfig);
        conf.setVersion(baseSourceVersion);
        return conf;
    }
}
