package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.RemoveOrphanTxfmr.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_REMOVE_ORPHAN_CONTACT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.cdl.RemoveOrphanConfig;
import com.latticeengines.spark.exposed.job.cdl.RemoveOrphanJob;


@Component(TRANSFORMER_NAME)
public class RemoveOrphanTxfmr extends ConfigurableSparkJobTxfmr<RemoveOrphanConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_REMOVE_ORPHAN_CONTACT;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<RemoveOrphanJob> getSparkJobClz() {
        return RemoveOrphanJob.class;
    }

    @Override
    protected Class<RemoveOrphanConfig> getJobConfigClz() {
        return RemoveOrphanConfig.class;
    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, RemoveOrphanConfig sparkJobConfig, //
                                     TransformerConfig configuration, List<Schema> baseSchemas) {
        int idx = (sparkJobConfig.getParentSrcIdx() != null && sparkJobConfig.getParentSrcIdx() == 0) ? 1 : 0;
        if (CollectionUtils.size(baseSchemas) > idx && baseSchemas.get(idx) != null) {
            Schema baseSchema = baseSchemas.get(idx);
            Map<String, Schema.Field> inputFields = new HashMap<>();
            baseSchema.getFields().forEach(field -> inputFields.putIfAbsent(field.name(), field));
            Schema resultSchema = AvroParquetUtils.parseAvroSchema(yarnConfiguration, result.getPath());
            return AvroUtils.overwriteFields(resultSchema, inputFields);
        } else {
            return null;
        }
    }

}
