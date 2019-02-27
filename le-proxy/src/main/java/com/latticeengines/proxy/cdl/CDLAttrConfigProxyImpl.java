package com.latticeengines.proxy.cdl;

import org.springframework.stereotype.Component;

import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

@Component("cdlAttrConfigProxy")
public class CDLAttrConfigProxyImpl extends BaseAttrConfigProxyImpl implements CDLAttrConfigProxy {

    protected CDLAttrConfigProxyImpl() {
        super("cdl");
    }

}
