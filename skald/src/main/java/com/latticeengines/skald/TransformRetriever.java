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
        JythonEvaluator evaluator = new JythonEvaluator(
                "def transform(args, record):\n    return sum(record.values())\n\n");
        return new JythonTransform(evaluator);
    }

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
