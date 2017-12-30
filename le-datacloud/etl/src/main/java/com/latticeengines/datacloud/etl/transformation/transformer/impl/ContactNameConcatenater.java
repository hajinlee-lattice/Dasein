package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_CONTACT_NAME_CONCATENATER;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.ContactNameConcatenateParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ContactNameConcatenateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(ContactNameConcatenater.TRANSFORMER_NAME)
public class ContactNameConcatenater
        extends AbstractDataflowTransformer<ContactNameConcatenateConfig, ContactNameConcatenateParameters> {
    private static final Logger log = LoggerFactory.getLogger(ContactNameConcatenater.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_CONTACT_NAME_CONCATENATER;
    public static final String DATAFLOW_BEAN_NAME = "ContactNameConcatenateFlow";
    public static final String DATA_CLOUD_VERSION_NAME = "DataCloudVersion";

    @Override
    protected String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ContactNameConcatenateConfig.class;
    }

    @Override
    protected Class<ContactNameConcatenateParameters> getDataFlowParametersClass() {
        return ContactNameConcatenateParameters.class;
    }

    @Override
    protected void updateParameters(ContactNameConcatenateParameters parameters, Source[] baseTemplates,
                                    Source targetTemplate, ContactNameConcatenateConfig configuration,
                                    List<String> baseVersions) {
        parameters.setRetainFields(configuration.getRetainFields());
        parameters.setConcatenateFields(configuration.getConcatenateFields());
        parameters.setResultField(configuration.getResultField());
    }
}
