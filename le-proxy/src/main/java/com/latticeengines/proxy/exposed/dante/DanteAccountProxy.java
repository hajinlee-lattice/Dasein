package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.network.exposed.dante.DanteAccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteAccountProxy")
public class DanteAccountProxy extends MicroserviceRestApiProxy implements DanteAccountInterface {

    public DanteAccountProxy() {
        super("/dante/accounts");
    }

    @SuppressWarnings("unchecked")
    public List<DanteAccount> getAccounts(int count, String customerSpace) {
        String url = constructUrl("/" + count + "?customerSpace=" + customerSpace);
        List list = get("getAccounts", url, List.class);
        return JsonUtils.convertList(list, DanteAccount.class);
    }
}
