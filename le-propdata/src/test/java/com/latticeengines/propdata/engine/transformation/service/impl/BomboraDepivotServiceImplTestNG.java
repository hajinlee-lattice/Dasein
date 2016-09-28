package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraDepivoted;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraDepivotConfiguration;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseInputSourceConfig;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component
public class BomboraDepivotServiceImplTestNG extends FixedIntervalTransformationServiceTestNGBase<BomboraDepivotConfiguration> {

    @Autowired
    BomboraDepivotedService refreshService;

    @Autowired
    BomboraDepivoted source;

    @Override
    TransformationService<BomboraDepivotConfiguration> getTransformationService() {
        return refreshService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    BomboraDepivotConfiguration createTransformationConfiguration() {
        BomboraDepivotConfiguration conf = new BomboraDepivotConfiguration();
        conf.setSourceName(source.getSourceName());
        conf.setSourceConfigurations(new HashMap<String, String>());
        BomboraFirehoseInputSourceConfig bomboraFirehoseInputSourceConfig = new BomboraFirehoseInputSourceConfig();
        bomboraFirehoseInputSourceConfig.setVersion(baseSourceVersion);
        conf.setBomboraFirehoseInputSourceConfig(bomboraFirehoseInputSourceConfig);
        conf.setVersion(baseSourceVersion);
        return conf;
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {}
}
