package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import static com.latticeengines.datacloud.etl.transformation.transformer.impl.SourceCopier.TRANSFORMER_NAME;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.dataflow.transformation.Copy;
import com.latticeengines.datacloud.etl.transformation.TransformerUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.CopierParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CopierConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(TRANSFORMER_NAME)
public class SourceCopier extends AbstractDataflowTransformer<CopierConfig, CopierParameters> {

    public static final String TRANSFORMER_NAME = "sourceCopier";

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
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return CopierConfig.class;
    }

    @Override
    protected Class<CopierParameters> getDataFlowParametersClass() {
        return CopierParameters.class;
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
        parameters.sortKeys = configuration.getSortKeys();
        parameters.sortDecending = Boolean.TRUE.equals(configuration.getSortDecending());
    }

    @Override
    protected void postDataFlowProcessing(TransformStep step, String workflowDir, CopierParameters paramters,
            CopierConfig configuration) {
        String avroGlob = PathUtils.toAvroGlob(workflowDir);
        if (configuration.getSortKeys() != null && !configuration.getSortKeys().isEmpty()) {
            TransformerUtils.removeAllButBiggestAvro(yarnConfiguration, avroGlob);
        } else {
            TransformerUtils.removeEmptyAvros(yarnConfiguration, avroGlob);
        }
    }

    private List<String> inputAttrs(Source source, String version) {
        String avroPath = TransformerUtils.avroPath(source, version, hdfsPathBuilder);
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroPath);
        return schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    }

}
