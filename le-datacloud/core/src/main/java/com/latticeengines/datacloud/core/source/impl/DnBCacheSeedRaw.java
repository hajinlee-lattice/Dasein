package com.latticeengines.datacloud.core.source.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.IngestionNames;
import com.latticeengines.datacloud.core.source.RawTransformationType;
import com.latticeengines.datacloud.core.source.TransformedToAvroSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.Path;

@Component("dnbCacheSeedRaw")
public class DnBCacheSeedRaw implements TransformedToAvroSource {

    private static final long serialVersionUID = -1260037352937689384L;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

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
        return null;
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
