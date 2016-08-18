package com.latticeengines.propdata.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.RawTransformationType;
import com.latticeengines.propdata.core.source.TransformedToAvroSource;

@Component("dnbCacheSeedRaw")
public class DnBCacheSeedRaw implements TransformedToAvroSource {

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    private String cronExpression;

    @Override
    public Path getHDFSPathToImportFrom() {
        return hdfsPathBuilder.constructIngestionDir(IngestionNames.DNB_CASHESEED);
    }

    @Override
    public String getSourceName() {
        return "DnBCacheSeedRaw";
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
        return "LAST_UPDATE_DATE";
    }

    @Override
    public String[] getPrimaryKey() {
        return new String[] { "DUNS_NUMBER", "LE_DOMAIN" };
    }

    @Override
    public String getTransformationServiceBeanName() {
        return "dnbCacheSeedIngestionService";
    }
}
