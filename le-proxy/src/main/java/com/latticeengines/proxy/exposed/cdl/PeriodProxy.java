package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Service;

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

    public List<String> getPeriodNames(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/names", shortenCustomerSpace(customerSpace));
        List<?> list = get("get period names", url, List.class);
        return JsonUtils.convertList(list, String.class);
    }

    public List<PeriodStrategy> getPeriodStrategies(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/strategies", shortenCustomerSpace(customerSpace));
        List<?> list = get("get period strategies", url, List.class);
        return JsonUtils.convertList(list, PeriodStrategy.class);
    }

    public BusinessCalendar getBusinessCalendar(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/calendar", shortenCustomerSpace(customerSpace));
        return get("get business calendar", url, BusinessCalendar.class);
    }

    public BusinessCalendar saveBusinessCalendar(String customerSpace, BusinessCalendar businessCalendar) {
        String url = constructUrl(URL_PREFIX + "/calendar", shortenCustomerSpace(customerSpace));
        return post("validate business calendar", url, businessCalendar, BusinessCalendar.class);
    }

    public void deleteBusinessCalendar(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/calendar", shortenCustomerSpace(customerSpace));
        delete("delete business calendar", url);
    }

    public String validateBusinessCalendar(String customerSpace, BusinessCalendar businessCalendar) {
        String url = constructUrl(URL_PREFIX + "/calendar/validate", shortenCustomerSpace(customerSpace));
        return post("validate business calendar", url, businessCalendar, String.class);
    }

    public String getEvaluationDate(String customerSpace) {
        String url = constructUrl(URL_PREFIX + "/evaluationdate", shortenCustomerSpace(customerSpace));
        return get("get evaluation date", url, String.class);
    }

    public List<String> getDateRange(String customerSpace, int year) {
        String url = constructUrl(URL_PREFIX + "/daterange/{year}", shortenCustomerSpace(customerSpace), year);
        List<?> dateRange = get("get start date and end date", url, List.class);
        return JsonUtils.convertList(dateRange, String.class);
    }
}
