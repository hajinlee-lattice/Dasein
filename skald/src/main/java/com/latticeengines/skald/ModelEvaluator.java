package com.latticeengines.skald;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ModelEvaluator {
    @RequestMapping(value = "ScoreRecord", method = RequestMethod.POST)
    public Map<String, Object> scoreRecord(@RequestBody ScoreRequest request) {
        log.info(String.format("Received a score request for %1$s model %2$s", request.spaceID, request.modelID));

        Map<String, Object> result = new HashMap<String, Object>();
        result.put("probability", 0.825);
        result.put("lift", 3.5);
        result.put("percentile", 96);
        result.put("bucket", "A");
        result.put("fake", true);

        return result;
    }

    private static final Log log = LogFactory.getLog(ModelEvaluator.class);
}