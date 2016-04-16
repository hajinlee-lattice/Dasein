package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.core.IngenstionNames;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.RawTransformationType;
import com.latticeengines.propdata.core.source.TransformedToAvroSource;

@Component("bomboraFirehose")
public class BomboraFirehose implements TransformedToAvroSource {

    private static final long serialVersionUID = -7091007703522407933L;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Value("${propdata.job.bomborafirehose.archive.schedule:0 0 13 * * *}")
    private String cronExpression;

    @Override
    public Path getHDFSPathToImportFrom() {
        return hdfsPathBuilder.constructIngestionDir(IngenstionNames.BOMBORA_FIREHOSE);
    }

    @Override
    public String getSourceName() {
        return IngenstionNames.BOMBORA_FIREHOSE;
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
}
