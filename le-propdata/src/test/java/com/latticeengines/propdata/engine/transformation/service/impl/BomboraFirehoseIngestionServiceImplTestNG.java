package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.source.impl.BomboraFirehose;
import com.latticeengines.propdata.engine.transformation.configuration.impl.BomboraFirehoseConfiguration;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

@Component
public class BomboraFirehoseIngestionServiceImplTestNG
        extends FirehoseTransformationServiceImplTestNGBase<BomboraFirehoseConfiguration> {

    @Autowired
    BomboraFirehoseIngestionService refreshService;

    @Autowired
    BomboraFirehose source;

    @Override
    TransformationService<BomboraFirehoseConfiguration> getTransformationService() {
        return refreshService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    BomboraFirehoseConfiguration createTransformationConfiguration() {
        List<String> versionsToProcess = new ArrayList<>();
        versionsToProcess.add(baseSourceVersion);
        BomboraFirehoseConfiguration conf = refreshService.createTransformationConfiguration(versionsToProcess, null);
        return conf;
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {}
}
