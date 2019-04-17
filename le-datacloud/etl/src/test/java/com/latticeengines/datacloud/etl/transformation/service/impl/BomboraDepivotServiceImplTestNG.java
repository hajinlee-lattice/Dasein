package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraDepivoted;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BomboraDepivotConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BomboraFirehoseInputSourceConfig;

@Component
public class BomboraDepivotServiceImplTestNG extends FixedIntervalTransformationServiceTestNGBase<BomboraDepivotConfiguration> {

    @Autowired
    BomboraDepivotedService refreshService;

    @Autowired
    BomboraDepivoted source;

    @Override
    protected TransformationService<BomboraDepivotConfiguration> getTransformationService() {
        return refreshService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected BomboraDepivotConfiguration createTransformationConfiguration() {
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
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {}
}
