package com.latticeengines.marketoadapter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.rest.DetailedErrors;

@RestController
@DetailedErrors
public class MarketoReceiverService {
    @RequestMapping(value = "MarketoReceiver", method = RequestMethod.POST)
    public Map<String, Object> receiveRecord(@RequestBody String input) {
        // Marketo sends the WebHook request with the x-www-form-urlencoded
        // content type, even though the content is unencoded JSON. Spring is
        // very unhappy with the sender lying about the content type, so
        // deserialization needs to be done manually. For strange and unknown
        // reasons Spring URL encodes the body, so we need to decode it before
        // further parsing.
        // TODO Try again to find ways to make Spring suck less.
        try {
            input = URLDecoder.decode(input, "UTF-8");
        } catch (UnsupportedEncodingException exc) {
            throw new RuntimeException("Unable to invoke URLDecoder with UTF-8", exc);
        }

        Map<String, Object> data;
        try {
            ObjectMapper serializer = new ObjectMapper();
            data = serializer.readValue(input, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException exc) {
            throw new RuntimeException("Unable to parse request as a JSON dictionary", exc);
        }

        if (!data.containsKey(keyField)) {
            throw new RuntimeException(keyField + " field was not present in the request");
        }
        if (!(data.get(keyField) instanceof String)) {
            throw new RuntimeException(keyField + " field value was not a string type");
        }

        // Marketo will pass default values rather than null for missing
        // information. Explicitly set those defaults to a sentinel value,
        // and replace them with null.
        for (String field : data.keySet()) {
            if (nullSentinel.equals(data.get(field))) {
                data.put(field, null);
            }
        }

        String key = (String) data.get(keyField);
        data.remove(keyField);
        Map<String, Object> result = dispatcher.receiveRecord(key, data);

        return result;
    }

    @Autowired
    private RecordDispatcher dispatcher;

    private static final String keyField = "LatticeKey";
    private static final String nullSentinel = "_null";
}