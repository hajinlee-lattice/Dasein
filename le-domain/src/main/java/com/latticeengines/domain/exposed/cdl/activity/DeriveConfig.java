package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;

public class DeriveConfig implements Serializable {
    private static final long serialVersionUID = 0L;

    public List<String> sourceAttrs; // col1, col2, ...
    public List<List<String>> patterns; // derivedName, pattern1, pattern2, ...

    public DeriveConfig() {
    }

    public DeriveConfig(List<String> sourceAttrs, List<List<String>> patterns) {
        this.sourceAttrs = sourceAttrs;
        this.patterns = patterns;
    }

    public String findDimensionId(List<String> vals) {
        String dimName = findDimensionName(vals);
        if (StringUtils.isNotBlank(dimName)) {
            return DimensionGenerator.hashDimensionValue(dimName);
        } else {
            return null;
        }
    }

    public String findDimensionPattern(List<String> vals) {
        for (List<String> patternConfig : patterns) {
            List<Pattern> matchers = patternConfig.subList(1, patternConfig.size()).stream().map(Pattern::compile)
                    .collect(Collectors.toList());
            assert matchers.size() == vals.size();
            Map<String, Pattern> valPatternMap = IntStream.range(0, vals.size()).boxed()
                    .collect(Collectors.toMap(vals::get, matchers::get));
            if (valPatternMap.entrySet().stream().allMatch(entry -> entry.getValue().matcher(entry.getKey()).matches())) {
                return String.join(",", patternConfig.subList(1, patternConfig.size()));
            }
        }
        return null;
    }

    public String findDimensionName(List<String> vals) {
        for (List<String> patternConfig : patterns) {
            List<Pattern> matchers = patternConfig.subList(1, patternConfig.size()).stream().map(Pattern::compile)
                    .collect(Collectors.toList());
            assert matchers.size() == vals.size();
            Map<String, Pattern> valPatternMap = IntStream.range(0, vals.size()).boxed()
                    .collect(Collectors.toMap(vals::get, matchers::get));
            if (valPatternMap.entrySet().stream().allMatch(entry -> entry.getValue().matcher(entry.getKey()).matches())) {
                return patternConfig.get(0);
            }
        }
        return null;
    }
}
