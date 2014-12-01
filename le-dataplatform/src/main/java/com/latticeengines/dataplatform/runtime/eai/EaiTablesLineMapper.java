package com.latticeengines.dataplatform.runtime.eai;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.springframework.batch.item.file.LineMapper;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.Table;

public class EaiTablesLineMapper implements LineMapper<List<Table>> {

    private static final Log log = LogFactory.getLog(EaiTablesLineMapper.class);
    /**
     * Interpret the line as a Json object and create a Map from it.
     *
     * @see LineMapper#mapLine(String, int)
     */
    @Override
    public List<Table> mapLine(String line, int lineNumber) throws Exception {
        List<Table> tables = null;
        JSONParser parser = new JSONParser();
        JSONArray objArr = (JSONArray) parser.parse(line);

        tables = new ArrayList<>();
        for (Object obj : objArr.toArray()) {
            tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
        }
        return tables;
    }
}
