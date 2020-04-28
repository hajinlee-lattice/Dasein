package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;

@Component("cdlAttrConfigProxy")
public class CDLAttrConfigProxyImpl extends BaseAttrConfigProxyImpl implements CDLAttrConfigProxy {

    protected CDLAttrConfigProxyImpl() {
        super("cdl");
    }

    @Override
    public AttributeSet getAttributeSet(String customerSpace, String name) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/attributeset",
                shortenCustomerSpace(customerSpace), name));
        return get("get attribute by name", url.toString(), AttributeSet.class);
    }

    @Override
    public List<AttributeSet> getAttributeSets(String customerSpace) {
        StringBuilder url = new StringBuilder();
        url.append(constructUrl("/customerspaces/{customerSpace}/attrconfig/attributeset",
                shortenCustomerSpace(customerSpace)));
        List<?> list = get("get attribute list", url.toString(), List.class);
        return JsonUtils.convertList(list, AttributeSet.class);
    }

    @Override
    public AttributeSet createOrUpdateAttributeSet(String customerSpace, AttributeSet attributeSet) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/attributeset", shortenCustomerSpace(customerSpace));
        return post("create or update attribute set", url, attributeSet, AttributeSet.class);
    }

    @Override
    public void deleteAttributeSet(String customerSpace, String name) {
        String url = constructUrl("/customerspaces/{customerSpace}/attrconfig/attributeset/{name}",
                shortenCustomerSpace(customerSpace), name);
        delete("Delete a attribute set", url);
    }
}
