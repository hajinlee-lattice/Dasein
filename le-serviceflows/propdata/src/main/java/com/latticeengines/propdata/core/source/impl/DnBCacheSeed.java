package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.RawTransformationType;
import com.latticeengines.propdata.core.source.TransformedToAvroSource;

@Component("dnbCacheSeed")
public class DnBCacheSeed implements TransformedToAvroSource {

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    private String cronExpression;

    @Override
    public Path getHDFSPathToImportFrom() {
        return hdfsPathBuilder.constructIngestionDir(IngestionNames.DNB_CASHESEED);
    }

    @Override
    public String getSourceName() {
        return IngestionNames.DNB_CASHESEED;
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
        return new String[] { "SeedID", "LE_Last_Upload_Date" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "dnbCacheSeedIngestionService";
    }
}
