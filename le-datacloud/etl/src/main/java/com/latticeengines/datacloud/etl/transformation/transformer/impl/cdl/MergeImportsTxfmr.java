package com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_MERGE_IMPORTS;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.ConfigurableSparkJobTxfmr;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.cdl.MergeImportsConfig;
import com.latticeengines.spark.exposed.job.cdl.MergeImportsJob;

@Component(MergeImportsTxfmr.TRANSFORMER_NAME)
public class MergeImportsTxfmr extends ConfigurableSparkJobTxfmr<MergeImportsConfig> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_MERGE_IMPORTS;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<MergeImportsJob> getSparkJobClz() {
        return MergeImportsJob.class;
    }

    @Override
    protected Class<MergeImportsConfig> getJobConfigClz() {
        return MergeImportsConfig.class;
    }

    @Override
    protected Schema getTargetSchema(HdfsDataUnit result, MergeImportsConfig sparkJobConfig,
            TransformerConfig configuration, List<Schema> baseSchemas) {
        if (CollectionUtils.isNotEmpty(baseSchemas)) {
            String avroGlob = PathUtils.toAvroGlob(result.getPath());
            Map<String, Schema.Field> inputFields = new HashMap<>();
            baseSchemas.forEach(schema -> {
                if (schema != null) {
                    schema.getFields().forEach(field -> inputFields.putIfAbsent(field.name(), field));
                }
            });
            Schema resultSchema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
            List<Schema.Field> fields = resultSchema.getFields().stream() //
                    .map(field -> {
                        Schema.Field srcField = inputFields.getOrDefault(field.name(), field);
                        return new Schema.Field(
                                srcField.name(),
                                srcField.schema(),
                                srcField.doc(),
                                srcField.defaultVal()
                        );
                    }) //
                    .collect(Collectors.toList());
            return Schema.createRecord(
                    resultSchema.getName(),
                    resultSchema.getDoc(),
                    resultSchema.getNamespace(),
                    false,
                    fields);
        } else {
            return null;
        }
    }

}
