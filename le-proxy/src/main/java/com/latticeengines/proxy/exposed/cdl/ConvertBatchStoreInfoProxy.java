package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreDetail;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreInfo;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("convertBatchStoreInfoProxy")
public class ConvertBatchStoreInfoProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/convertbatchstoreinfo";

    protected ConvertBatchStoreInfoProxy() {
        super("cdl");
    }

    public ConvertBatchStoreInfo createConvertBatchStoreInfo(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/create", shortenCustomerSpace(customerSpace));
        return post("Create ConvertBatchStoreInfo record", url, null, ConvertBatchStoreInfo.class);
    }

    public ConvertBatchStoreInfo getConvertBatchStoreInfo(String customerSpace, Long pid) {
        String url = constructUrl(URL_PREFIX + "/get/{pid}", shortenCustomerSpace(customerSpace), pid);
        return get("Get ConvertBatchStoreInfo record by pid", url, ConvertBatchStoreInfo.class);
    }

    public void updateDetail(String customerSpace, Long pid, ConvertBatchStoreDetail detail) {
        String url = constructUrl(URL_PREFIX + "/update/{pid}/detail",
                shortenCustomerSpace(customerSpace), pid);
        put("Update ConvertBatchStoreInfo record detail", url, detail, Void.class);
    }
}
