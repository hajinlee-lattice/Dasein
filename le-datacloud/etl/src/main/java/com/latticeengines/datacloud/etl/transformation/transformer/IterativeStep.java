package com.latticeengines.datacloud.etl.transformation.transformer;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;

public class IterativeStep extends TransformStep {

    private static final Logger log = LoggerFactory.getLogger(IterativeStep.class);
    private static final ObjectMapper OM = new ObjectMapper();

    public static final Integer MAX_ITERATION = 10;

    public static final String CTX_ITERATION = "Iteration";
    public static final String CTX_LAST_COUNT = "LastCount";

    private IterativeStepConfig strategy;

    public IterativeStep(String name, Transformer transformer, Source[] baseSources, List<String> baseVersions,
            Source[] baseTemplates, Source target, String targetVersion, Source targetTemplate, String config) {
        super(name, transformer, baseSources, baseVersions, baseTemplates, target, targetVersion, targetTemplate,
                config);
        try {
            JsonNode configJson = OM.readTree(config);
            strategy = OM.treeToValue(configJson.get(IterativeStepConfig.ITERATE_STRATEGY), IterativeStepConfig.class);
            if (StringUtils.isBlank(strategy.getIteratingSource())) {
                throw new IllegalArgumentException("Must provide one iterating source for iterative step.");
            }
            boolean hasIteratingSource = false;
            for (Source source : baseSources) {
                if (source.getSourceName().equals(strategy.getIteratingSource())) {
                    hasIteratingSource = true;
                    break;
                }
            }
            if (!hasIteratingSource) {
                throw new IllegalArgumentException("Cannot find iterating source " + strategy.getIteratingSource()
                        + " from provided base source. This will lead to infinite iteration.");
            }
            log.info("Parsed iterative strategy: \n" + JsonUtils.pprint(strategy));
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse iterative strategy from config: " + config, e);
        }
    }

    public void setBaseSources(Source[] baseSources) {
        this.baseSources = baseSources;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public IterativeStepConfig getStrategy() {
        return strategy;
    }

    public void setStrategy(IterativeStepConfig strategy) {
        this.strategy = strategy;
    }
}
