package com.latticeengines.proxy.exposed.dante;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.network.exposed.dante.DanteLeadInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteLeadProxy")
public class DanteLeadProxy extends MicroserviceRestApiProxy implements DanteLeadInterface {

    public DanteLeadProxy() {
        super("/dante/leads");
    }

    public void create(Recommendation recommendation, String customerSpace) {
        String url = constructUrl("?customerSpace=" + customerSpace);
        post("create", url, recommendation, Object.class);
    }
}
