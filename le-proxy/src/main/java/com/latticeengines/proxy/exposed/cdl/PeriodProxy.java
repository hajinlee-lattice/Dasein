package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Service("periodProxy")
public class PeriodProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    private static final String URL_PREFIX = "/customerspaces/{customerSpace}/periods";

    protected PeriodProxy() {
        super("cdl");
    }

    public List<String> getPeriodNames(@PathVariable String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/names", shortenCustomerSpace(customerSpace));
        List list = get("get period names", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public List<PeriodStrategy> getPeriodStrategies(@PathVariable String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/strategies", shortenCustomerSpace(customerSpace));
        List list = get("get period strategies", url, List.class);
        return JsonUtils.convertList(list, PeriodStrategy.class);
    }

    public BusinessCalendar getBusinessCalendar(@PathVariable String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/calendar", shortenCustomerSpace(customerSpace));
        return get("get business calendar", url, BusinessCalendar.class);
    }

    public BusinessCalendar saveBusinessCalendar(@PathVariable String customerSpace,
                                                 @RequestBody BusinessCalendar businessCalendar) {
        String url = constructUrl(URL_PREFIX + "/calendar", shortenCustomerSpace(customerSpace));
        return post("validate business calendar", url, businessCalendar, BusinessCalendar.class);
    }

    public String validateBusinessCalendar(@PathVariable String customerSpace,
                                           @RequestBody BusinessCalendar businessCalendar) {
        String url = constructUrl(URL_PREFIX + "/calendar/validate", shortenCustomerSpace(customerSpace));
        return post("validate business calendar", url, businessCalendar, String.class);
    }


}
