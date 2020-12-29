package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.DeleteRequest;

@Component("plsCDLSoftDeleteProxy")
public class PlsCDLSoftDeleteProxy extends PlsRestApiProxyBase {
    public PlsCDLSoftDeleteProxy() {
        super("pls/cdl/soft-delete");
    }

    public void delete(DeleteRequest deleteRequest) {
        String url = constructUrl("/");
        post("delete entity", url, deleteRequest, Map.class);
    }
}
