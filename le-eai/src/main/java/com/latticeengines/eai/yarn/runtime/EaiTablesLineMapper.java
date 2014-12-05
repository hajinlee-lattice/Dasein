package com.latticeengines.eai.yarn.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.batch.item.file.LineMapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;

public class EaiTablesLineMapper implements LineMapper<ImportConfiguration> {

    private static final Log log = LogFactory.getLog(EaiTablesLineMapper.class);
    /**
     * Interpret the line as a Json object and create a Map from it.
     *
     * @see LineMapper#mapLine(String, int)
     */
    @Override
    public ImportConfiguration mapLine(String line, int lineNumber) throws Exception {
        log.info("Parsing line " + line);
        JSONParser parser = new JSONParser();
        JSONObject jsonObj = (JSONObject) parser.parse(line);
        return JsonUtils.deserialize(jsonObj.toString(), ImportConfiguration.class);
    }
}
