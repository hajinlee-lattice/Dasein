package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY_TXMFR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.common.CopyConfig;
import com.latticeengines.spark.exposed.job.common.CopyJob;


@Component(CopyTxfmr.TRANSFORMER_NAME)
public class CopyTxfmr extends ConfigurableSparkJobTxfmr<CopyConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_COPY_TXMFR;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<CopyJob> getSparkJobClz() {
        return CopyJob.class;
    }

    @Override
    protected Class<CopyConfig> getJobConfigClz() {
        return CopyConfig.class;
    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, CopyConfig sparkJobConfig, //
                                     TransformerConfig configuration, List<Schema> baseSchemas) {
        Schema baseSchema = baseSchemas.get(0);
        Map<String, String> renameMap = sparkJobConfig.getRenameAttrs();
        Map<String, Schema.Field> inputFields = new HashMap<>();
        baseSchema.getFields().forEach(field -> {
            String fieldName = field.name();
            if (MapUtils.isNotEmpty(renameMap)) {
                fieldName = renameMap.getOrDefault(fieldName, fieldName);
            }
            inputFields.putIfAbsent(fieldName, field);
        });
        Schema resultSchema = AvroParquetUtils.parseAvroSchema(yarnConfiguration, result.getPath());
        return AvroUtils.overwriteFields(resultSchema, inputFields);
    }

}
