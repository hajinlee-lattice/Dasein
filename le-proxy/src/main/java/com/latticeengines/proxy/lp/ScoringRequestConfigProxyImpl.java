package com.latticeengines.proxy.lp;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ScoringRequestConfigContext;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.ScoringRequestConfigProxy;

@Component("scoringRequestConfigProxy")
public class ScoringRequestConfigProxyImpl extends MicroserviceRestApiProxy implements ScoringRequestConfigProxy {

    protected ScoringRequestConfigProxyImpl() {
        super("lp");
    }

    @Override
    public ScoringRequestConfigContext retrieveScoringRequestConfigContext(String configUuid) {
        if (StringUtils.isBlank(configUuid)) {
            throw new LedpException(LedpCode.LEDP_18194, new String[]{configUuid});
        }
        String url = constructUrl("/scoringrequestconfig/{configUuid}", configUuid);
        return get("find scoring request config by config uuid", url, ScoringRequestConfigContext.class);
    }

}
