package com.latticeengines.skald;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.jython.JythonEvaluator;

@Service
public class TransformRetriever {
    public JythonTransform getTransform(String name) {
        log.info("Retrieving jython transform " + name);
        // TODO Implement a caching strategy for these.

        // TODO Actually retrieve these from somewhere.
        String path = "com/latticeengines/domain/exposed/transforms/python/" + name + ".py";
        return new JythonTransform(JythonEvaluator.fromResource(path));
    }

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
