package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceCopier.TRANSFORMER_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_COPY;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.dataflow.transformation.Copy;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.CopierParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.CopierConfig;
import com.latticeengines.domain.exposed.metadata.Table;

@Component(TRANSFORMER_NAME)
public class SourceCopier extends AbstractDataflowTransformer<CopierConfig, CopierParameters> {

    public static final String TRANSFORMER_NAME = TRANSFORMER_COPY;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public String getDataFlowBeanName() {
        return Copy.BEAN_NAME;
    }

    @Override
    protected boolean validateConfig(CopierConfig config, List<String> sourceNames) {
        return true;
    }

    @Override
    protected void preDataFlowProcessing(TransformStep step, String workflowDir, CopierParameters parameters,
                                         CopierConfig configuration) {
        List<String> retainAttrs = configuration.getRetainAttrs();
        if (retainAttrs != null && !retainAttrs.isEmpty()) {
            Source source = step.getBaseSources()[0];
            String version = step.getBaseVersions().get(0);
            List<String> inputAttrs = inputAttrs(source, version);
            if (inputAttrs.size() > 2 * retainAttrs.size()) {
                // use retain attrs
                parameters.retainAttrs = retainAttrs;
            } else {
                // use discard attrs
                inputAttrs.removeAll(retainAttrs);
                parameters.discardAttrs = inputAttrs;
            }
        }
    }

    private List<String> inputAttrs(Source source, String version) {
        String avroPath;
        if (source instanceof TableSource) {
            Table table = ((TableSource) source).getTable();
            avroPath = table.getExtracts().get(0).getPath();
        } else {
            String avroDir = hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), version).toString();
            avroPath = avroDir + "/*.avro";
        }
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroPath);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    }

}
