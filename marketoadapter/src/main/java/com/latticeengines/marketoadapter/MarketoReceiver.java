package com.latticeengines.marketoadapter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.DetailedErrors;

@RestController
@DetailedErrors
public class MarketoReceiver {
    @RequestMapping(value = "MarketoReceiver", method = RequestMethod.POST)
    public String receiveRecord(@RequestBody Map<String, Object> data) {
        if (!data.containsKey(keyField)) {
            throw new RuntimeException(keyField + " field was not present in the request");
        }
        if (!(data.get(keyField) instanceof String)) {
            throw new RuntimeException(keyField + " field value was not a string type");
        }

        // Marketo will pass default values rather than null for missing
        // information.
        // Explicitly set those defaults to a sentinel value, and replace them
        // with null.
        for (String field : data.keySet()) {
            if (nullField.equals(data.get(field))) {
                data.put(field, null);
            }
        }

        String key = (String) data.get(keyField);
        data.remove(keyField);
        String result = dispatcher.receiveRecord(key, data);

        return result;
    }

    @Autowired
    private RecordDispatcher dispatcher;

    private static final String keyField = "LatticeKey";
    private static final String nullField = "_null";

    private static final Log log = LogFactory.getLog(MarketoReceiver.class);
}