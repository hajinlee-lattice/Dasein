package com.latticeengines.datacloud.core.source.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.RawTransformationType;
import com.latticeengines.datacloud.core.source.TransformedToAvroSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.Path;

@Component("bomboraFirehose")
public class BomboraFirehose implements TransformedToAvroSource {

    private static final long serialVersionUID = -7091007703522407933L;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.job.bomborafirehose.archive.schedule:0 0 13 * * *}")
    private String cronExpression;

    @Override
    public Path getHDFSPathToImportFrom() {
        return hdfsPathBuilder.constructIngestionDir(IngestionNames.BOMBORA_FIREHOSE);
    }

    @Override
    public String getSourceName() {
        return IngestionNames.BOMBORA_FIREHOSE;
    }

    @Override
    public String getDefaultCronExpression() {
        return cronExpression;
    }

    @Override
    public RawTransformationType getTransformationType() {
        return RawTransformationType.CSV_TARGZ_TO_AVRO;
    }

    @Override
    public String getTimestampField() {
        return "LE_Last_Upload_Date";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "ID", "LE_Last_Upload_Date" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "bomboraFirehoseIngestionService";
    }
}
