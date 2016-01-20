package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.LPFunctions;

@Component
public class LPFunctionsImpl implements LPFunctions {

    @Override
    public AbstractMap.SimpleImmutableEntry<String, String> getLPTemplateTypeAndVersion(
            ConnectionMgr conn_mgr) throws IOException, RuntimeException {
        AbstractMap.SimpleImmutableEntry<String, String> result = new AbstractMap.SimpleImmutableEntry<>("ELQ","2.2.1");
        return result;
    }

    @Override
    public Boolean addLDCMatch(ConnectionMgr conn_mgr, String source, String lp_template_version)
            throws IOException, RuntimeException {
        return Boolean.FALSE;
    }

    @Override
    public Map<String, String> getLDCWritebackAttributes(ConnectionMgr conn_mgr)
            throws IOException, RuntimeException {
        Map<String, String> result = new HashMap<>();
        result.put("column_name_in_propdata", "column_name_in_customer_system");
        return result;
    }

    @Override
    public Boolean setLDCWritebackAttributes(ConnectionMgr conn_mgr, String source,
            Map<String, String> attributes, String lp_template_version)
            throws IOException, RuntimeException {
        return Boolean.FALSE;
    }

    @Override
    public Boolean setLDCWritebackAttributesDefaultName(ConnectionMgr conn_mgr, String source,
            Set<String> column_names_in_propdata, String lp_template_type, 
            String lp_template_version) throws IOException, RuntimeException {
        return Boolean.FALSE;
    }
}
