package com.latticeengines.skald;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

@Controller
public class MarketoReceiver
{
    @RequestMapping(value = "MarketoReceiver", method = RequestMethod.POST)
    @ResponseBody
    public String receiveRecord(@RequestBody String input)
    {
        // Marketo sometimes sends the post data with URL encoding.
        if (input.startsWith("%"))
        {
            try
            {
                input = URLDecoder.decode(input, "UTF-8");
            }
            catch (UnsupportedEncodingException exc)
            {
                log.error("Unable to invoke URLDecoder with UTF-8");
                throw new RuntimeException("Encountered an internal system error");
            }
        }
        
        // Handle the dictionary deserialization explicitly, because even if Jackson can
        // be made to do it within Spring, all the deserializations errors are swallowd.
        Map<String, Object> data;
        try
        {
            ObjectMapper serializer = new ObjectMapper();
            data = serializer.readValue(input, new TypeReference<Map<String, Object>>() {});
        }
        catch (IOException exc)
        {
            throw new RuntimeException("Unable to parse request as a JSON dictionary", exc);
        }
        
        if (!data.containsKey(keyField))
        {
            throw new RuntimeException(keyField + " field was not present in the request");
        }
        if (!(data.get(keyField) instanceof String))
        {
            throw new RuntimeException(keyField + " field value was not a string type");
        }
        
        String key = (String)data.get(keyField);
        data.remove(keyField);
        String result = dispatcher.receiveRecord(key, data);

        return result;
    }
    
    @Autowired
    private RecordDispatcher dispatcher;
    
    private static final String keyField = "LatticeKey";
    
    private static final Log log = LogFactory.getLog(MarketoReceiver.class);
}