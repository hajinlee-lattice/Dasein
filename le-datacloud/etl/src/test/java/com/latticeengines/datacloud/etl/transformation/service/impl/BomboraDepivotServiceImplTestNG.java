package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraDepivoted;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BomboraDepivotConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BomboraFirehoseInputSourceConfig;

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
