package com.latticeengines.propdata.collection.dataflow.pivot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotResult;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotType;

import cascading.tuple.TupleEntry;

public class BuiltWithPivotStrategy implements PivotStrategy {

    private static final long serialVersionUID = 6019495135106360664L;

    public static String[] typeFlags = {
            "BusinessTechnologiesMarketingAutomation",
            "BusinessTechnologiesSocialMediaPresence",
            "BusinessTechnologiesOpenSourceAdoption"
    };

    @Override
    public List<PivotResult> pivot(TupleEntry arguments) {
        List<PivotResult> results = new ArrayList<>();
        String tech = arguments.getString("Technology_Name");
        if (hasMarketingAutomation(tech)) {
            PivotResult result = new PivotResult("BusinessTechnologiesMarketingAutomation", PivotType.EXISTS);
            result.setValue(true);
            results.add(result);
        }

        if (hasSocialMediaPresence(tech)) {
            PivotResult result = new PivotResult("BusinessTechnologiesSocialMediaPresence", PivotType.EXISTS);
            result.setValue(true);
            results.add(result);
        }

        String tag = arguments.getString("Technology_Tag").toLowerCase();

        if (hasOpenSource(tech, tag)) {
            PivotResult result = new PivotResult("BusinessTechnologiesOpenSourceAdoption", PivotType.EXISTS);
            result.setValue(true);
            results.add(result);
        }

        return results;
    }

    @Override
    public Map<String, Object> getDefaultValues() {
        Map<String, Object> toReturn = new HashMap<>();
        for (String flag: typeFlags) {
            toReturn.put(flag, false);
        }
        return toReturn;
    }

    @Override
    public List<DataFlowBuilder.FieldMetadata> getFieldMetadataList() {
        List<DataFlowBuilder.FieldMetadata> fms = new ArrayList<>();
        for (String flag: typeFlags) {
            fms.add(new DataFlowBuilder.FieldMetadata(flag, Boolean.class));
        }
        return fms;
    }

    @Override
    public Set<String> getResultColumns() {
        return new HashSet<>(Arrays.asList(typeFlags));
    }

    private boolean hasMarketingAutomation(String tech) {
        return Arrays.asList("eloqua","marketo","act-on","pardot","hubspot","vtrenz (silverpop)")
                .contains(tech.toLowerCase());
    }

    private boolean hasSocialMediaPresence(String tech) {
        if (StringUtils.isEmpty(tech)) return false;
        String lowerTech = tech.toLowerCase();
        return lowerTech.contains("facebook") || lowerTech.contains("twitter") || lowerTech.contains("linkedin");
    }

    private boolean hasOpenSource(String tech, String tag) {
        return "framework".equalsIgnoreCase(tag) || (
                "web server".equalsIgnoreCase(tag) && (
                        tech.toLowerCase().contains("apache") || tech.toLowerCase().contains("nginx")
                ));
    }
}
