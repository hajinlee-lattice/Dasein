package com.latticeengines.proxy.exposed.pls;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;


@Component("plsHealthCheckProxy")
public class PlsHealthCheckProxyImpl extends BaseRestApiProxy implements PlsHealthCheckProxy {

    private static final Logger log = LoggerFactory.getLogger(PlsHealthCheckProxyImpl.class);


    public PlsHealthCheckProxyImpl() {
        super(PropertyUtils.getProperty("cdl.app.public.url"), "pls");
    }

    @Override
    public StatusDocument systemCheck() {
        try {
            String url = constructUrl("/health/systemstatus");
            return get("systemCheck", url, StatusDocument.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31115, new String[]{e.getMessage()});
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> getActiveStack() {
        try {
            String url = constructUrl("/health/stackinfo");
            return get("getActiveStack", url, Map.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_31115, new String[]{e.getMessage()});
        }
    }

}
