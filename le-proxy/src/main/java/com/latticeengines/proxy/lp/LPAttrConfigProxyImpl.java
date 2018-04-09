package com.latticeengines.proxy.lp;

import org.springframework.stereotype.Component;

import com.latticeengines.proxy.cdl.BaseAttrConfigProxyImpl;
import com.latticeengines.proxy.exposed.lp.LPAttrConfigProxy;

@Component("lpAttrConfigProxy")
public class LPAttrConfigProxyImpl extends BaseAttrConfigProxyImpl implements LPAttrConfigProxy {

    protected LPAttrConfigProxyImpl() {
        super("lp");
    }

}
