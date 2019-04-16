package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.SampleTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("configurableSampleFlow")
public class ConfigurableSampleFlow extends ConfigurableFlowBase<SampleTransformerConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SampleTransformerConfig config = getTransformerConfig(parameters);

        Node sampled = addSource(parameters.getBaseTables().get(0));

        String filter = config.getFilter();
        List<String> attrs = sampled.getFieldNames();
        if (filter != null) {
            sampled = sampled.filter(filter, new FieldList(config.getFilterFields()));
        }

        Float fraction = config.getFraction();

        if (fraction != null) {
            sampled = sampled.sample(config.getFraction());
        }

        List<String> reportAttrs = config.getReportAttrs();

        if (reportAttrs != null) {
            attrs = reportAttrs;
        }

        List<String> excludeAttrs = config.getExcludeAttrs();
        if (excludeAttrs != null) {
            HashSet<String> excludeSet = new HashSet<String>(excludeAttrs);

            List<String> finalAttrs = new ArrayList<String>();
            for (String attr : attrs) {
                if (!excludeSet.contains(attr)) {
                    finalAttrs.add(attr);
                }
            }
            attrs = finalAttrs;
        }

        sampled = sampled.retain(new FieldList(attrs.toArray(new String[attrs.size()])));

        return sampled;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SampleTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "configurableSampleFlow";
    }

    @Override
    public String getTransformerName() {
        return "sampler";

    }
}
