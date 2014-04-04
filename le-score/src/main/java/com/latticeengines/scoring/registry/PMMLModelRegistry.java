package com.latticeengines.scoring.registry;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.dmg.pmml.PMML;
import org.springframework.stereotype.Component;

@Component("pmmlModelRegistry")
public class PMMLModelRegistry {

    private Map<String, PMML> models = new TreeMap<String, PMML>(new Comparator<String>() {

        @Override
        public int compare(String left, String right) {
            return (left).compareToIgnoreCase(right);
        }

    });

    public Set<String> idSet() {
        return Collections.unmodifiableSet(models.keySet());
    }

    public PMML get(String id) {
        return models.get(id);
    }

    public PMML put(String id, PMML pmml) {
        return models.put(id, pmml);
    }

    public PMML remove(String id) {
        return models.remove(id);
    }
}
