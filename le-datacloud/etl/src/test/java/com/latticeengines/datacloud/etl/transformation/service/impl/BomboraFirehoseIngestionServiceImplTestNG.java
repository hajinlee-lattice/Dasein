package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraFirehose;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BomboraFirehoseConfiguration;

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
