package com.latticeengines.proxy.exposed.dante;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dante.DanteLeadDTO;
import com.latticeengines.network.exposed.dante.DanteLeadInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteLeadProxy")
public class DanteLeadProxy extends MicroserviceRestApiProxy implements DanteLeadInterface {

    public DanteLeadProxy() {
        super("/dante/leads");
    }

    public void create(DanteLeadDTO danteLeadDTO, String customerSpace) {
        String url = constructUrl("?customerSpace=" + customerSpace);
        post("create", url, danteLeadDTO, Object.class);
    }
}
