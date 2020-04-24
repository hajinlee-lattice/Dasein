package com.latticeengines.domain.exposed.cdl.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelFeatureImportance;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

import reactor.core.publisher.Flux;

@Component
public class FeatureImportanceMgr {

    private static final Logger log = LoggerFactory.getLogger(FeatureImportanceMgr.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${cdl.feature.importance.transform.suffixes}")
    private String featureImportanceTransformSuffixes;

    @Value("${cdl.feature.importance.special.suffix}")
    private String featureImportanceSpecialSuffix;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private class AttrFeatureImportance {
        private double featureImportance;
        private String attributeName;

        AttrFeatureImportance(double featureImportance, String attributeName) {
            this.featureImportance = featureImportance;
            this.attributeName = attributeName;
        }

        public double getFeatureImportance() {
            return featureImportance;
        }

        public String getAttributeName() {
            return attributeName;
        }
    }

    private List<String> getFeatureImportanceTransformSuffixes() {
        return Arrays.asList(featureImportanceTransformSuffixes.split(","));
    }

    public Map<String, Integer> getFeatureImportance(String customerSpace, ModelSummary modelSummary) {

        StopWatch stopWatch = new StopWatch();
        StopWatch splitter = new StopWatch();
        try {
            stopWatch.start();
            splitter.start();
            String featureImportanceRaw = retrieveFeatureImportances(customerSpace, modelSummary);
            log.info(String.format("Feature Importance Compilation (Split: %d ms Total: %d ms): Raw file procurement",
                    splitter.getTime(), stopWatch.getTime()));
            stopWatch.suspend();
            splitter.reset();

            stopWatch.resume();
            splitter.start();
            List<AttrFeatureImportance> attrFI = Arrays.stream(featureImportanceRaw.split("\n")).map(line -> {
                try {
                    return new AttrFeatureImportance(Double.parseDouble(line.split(",")[1]), line.split(",")[0]);
                } catch (NumberFormatException e) {
                    // ignore since this is for the first row
                    return new AttrFeatureImportance(Double.MIN_VALUE, null);
                }
            }).filter(x -> x.getFeatureImportance() != Double.MIN_VALUE)
                    .sorted(Comparator.comparingDouble(AttrFeatureImportance::getFeatureImportance).reversed())
                    .collect(Collectors.toList());

            AtomicInteger i = new AtomicInteger(1);
            Map<String, Integer> importanceOrdering = Flux.fromIterable(attrFI).collect(HashMap<String, Integer>::new,
                    (map, afi) -> map.put(afi.getAttributeName(), i.getAndIncrement())).block();

            log.info(String.format(
                    "Feature Importance Compilation (Split: %d ms Total: %d ms): Extraction from raw file",
                    splitter.getTime(), stopWatch.getTime()));
            stopWatch.suspend();
            splitter.reset();

            stopWatch.resume();
            splitter.start();
            return convertDerivedAttrNamesToParentAttrNames(importanceOrdering);
        } catch (Exception e) {
            log.error("Unable to populate feature importance due to " + e.getLocalizedMessage());
            return new HashMap<>();
        } finally {
            log.info(String.format(
                    "Feature Importance Compilation (Split: %d ms Total: %d ms): Flattening into parent attributes",
                    splitter.getTime(), stopWatch.getTime()));
        }
    }

    private String retrieveFeatureImportances(String customerSpace, ModelSummary modelSummary) throws IOException {
        String featureImportanceFilePathPattern = "{0}/{1}/models/{2}/{3}/{4}/rf_model.txt";
        String[] filePathParts = modelSummary.getLookupId().split("\\|");
        String featureImportanceFilePath = MessageFormat.format(featureImportanceFilePathPattern, //
                modelingServiceHdfsBaseDir, // 0
                filePathParts[0], // 1
                filePathParts[1], // 2
                filePathParts[2], // 3
                ApplicationIdUtils.stripJobId(modelSummary.getApplicationId())); // 4
        featureImportanceFilePath = getS3Path(customerSpace, featureImportanceFilePath);

        log.info("Feature Importance Compilation (Split: 0 ms Total: 0 ms): Start");
        if (!HdfsUtils.fileExists(yarnConfiguration, featureImportanceFilePath)) {
            log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
            throw new LedpException(LedpCode.LEDP_10011, new String[] { featureImportanceFilePath });
        }
        log.info("Attempting to get feature importance from the file: " + featureImportanceFilePath);
        String featureImportanceRaw = HdfsUtils.getHdfsFileContents(yarnConfiguration, featureImportanceFilePath);
        if (StringUtils.isEmpty(featureImportanceRaw)) {
            log.error("Failed to find the feature importance file: " + featureImportanceFilePath);
            throw new LedpException(LedpCode.LEDP_40037,
                    new String[] { featureImportanceFilePath, modelSummary.getId(), customerSpace });
        }
        return featureImportanceRaw;
    }

    private String getS3Path(String customerSpace, String featureImportanceFilePath) throws IOException {
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String s3Path = pathBuilder.exploreS3FilePath(featureImportanceFilePath, s3Bucket);
        if (HdfsUtils.fileExists(yarnConfiguration, s3Path)) {
            featureImportanceFilePath = s3Path;
        }
        return featureImportanceFilePath;
    }

    public List<ModelFeatureImportance> getModelFeatureImportance(String customerSpace, ModelSummary modelSummary) {
        List<ModelFeatureImportance> importances = new ArrayList<>();
        try {
            String featureImportanceRaw = retrieveFeatureImportances(customerSpace, modelSummary);
            List<String> lines = Arrays.stream(featureImportanceRaw.split("\n")).collect(Collectors.toList());
            for (int i = 0; i < lines.size(); i++) {
                if (i == 0)
                    continue;
                String[] tokens = StringUtils.split(lines.get(i), ",", 3);
                if (tokens.length > 2) {
                    importances.add(new ModelFeatureImportance(modelSummary, tokens[0], Double.parseDouble(tokens[1]),
                            StringUtils.strip(tokens[2], " \"")));
                } else {
                    importances.add(new ModelFeatureImportance(modelSummary, tokens[0], Double.parseDouble(tokens[1])));
                }
            }
        } catch (Exception e) {
            log.error(String.format("Unable to populate feature importance for %s, modelId=%s, due to %s",
                    customerSpace, modelSummary.getId(), e.toString()));
        }
        return importances;
    }

    private Map<String, Integer> convertDerivedAttrNamesToParentAttrNames(Map<String, Integer> importanceOrdering) {
        Set<String> attributeNames = importanceOrdering.keySet();
        Map<String, Integer> toReturn = new HashMap<>();

        attributeNames.forEach(attrName -> {
            if (attrName.contains(featureImportanceSpecialSuffix)) {
                // find all with range replace them all with parent with the
                // least importance
                String parentAttributeName = attrName.substring(0, attrName.indexOf(featureImportanceSpecialSuffix));
                String mostImportantDerivedAttr = attributeNames.stream()
                        .filter(attr -> attr.startsWith(parentAttributeName)) //
                        .reduce(parentAttributeName,
                                (attr1, attr2) -> importanceOrdering.getOrDefault(attr1,
                                        Integer.MAX_VALUE) <= importanceOrdering.getOrDefault(attr2, Integer.MAX_VALUE)
                                                ? attr1
                                                : attr2);
                toReturn.put(parentAttributeName, importanceOrdering.get(mostImportantDerivedAttr));
            } else if (getFeatureImportanceTransformSuffixes().stream().anyMatch(
                    suffix -> attrName.endsWith(suffix) && !attrName.contains(featureImportanceSpecialSuffix))) {
                String matchedSuffix = getFeatureImportanceTransformSuffixes().stream().filter(attrName::contains)
                        .findFirst().get();
                String parentAttributeName = attrName.replace(matchedSuffix, "");
                toReturn.put(parentAttributeName,
                        Math.min(importanceOrdering.getOrDefault(parentAttributeName, Integer.MAX_VALUE),
                                importanceOrdering.get(attrName)));
            } else {
                toReturn.put(attrName, importanceOrdering.get(attrName));
            }

        });
        return toReturn;
    }
}
