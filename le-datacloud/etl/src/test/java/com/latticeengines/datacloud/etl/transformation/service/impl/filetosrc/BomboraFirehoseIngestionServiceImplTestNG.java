package com.latticeengines.datacloud.etl.transformation.service.impl.filetosrc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.BomboraFirehose;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.BomboraFirehoseIngestionService;
import com.latticeengines.datacloud.etl.transformation.service.impl.FirehoseTransformationServiceImplTestNGBase;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.BomboraFirehoseConfiguration;

@Deprecated
public class BomboraFirehoseIngestionServiceImplTestNG
        extends FirehoseTransformationServiceImplTestNGBase<BomboraFirehoseConfiguration> {

    @Inject
    BomboraFirehoseIngestionService refreshService;

    @Inject
    BomboraFirehose source;

    @Override
    protected TransformationService<BomboraFirehoseConfiguration> getTransformationService() {
        return refreshService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected BomboraFirehoseConfiguration createTransformationConfiguration() {
        List<String> versionsToProcess = new ArrayList<>();
        versionsToProcess.add(baseSourceVersion);
        BomboraFirehoseConfiguration conf = refreshService.createTransformationConfiguration(versionsToProcess, null);
        return conf;
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {}
}
