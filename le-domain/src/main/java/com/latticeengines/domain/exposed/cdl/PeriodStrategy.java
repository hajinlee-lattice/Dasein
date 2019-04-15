package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PeriodStrategy implements Serializable {

    public static final ImmutableList<PeriodStrategy> NATURAL_PERIODS = ImmutableList.of( //
            new PeriodStrategy(Template.Week), //
            new PeriodStrategy(Template.Month), //
            new PeriodStrategy(Template.Quarter), //
            new PeriodStrategy(Template.Year));
    private static final long serialVersionUID = 4453849238385668063L;
    public static PeriodStrategy CalendarWeek, CalendarMonth, CalendarQuarter, CalendarYear;

    static {
        CalendarWeek = new PeriodStrategy(Template.Week);
        CalendarMonth = new PeriodStrategy(Template.Month);
        CalendarQuarter = new PeriodStrategy(Template.Quarter);
        CalendarYear = new PeriodStrategy(Template.Year);
    }

    @JsonProperty("template")
    private Template template;
    @JsonProperty("name")
    private String name;
    @JsonProperty("start_time")
    private String startTimeStr;
    @JsonProperty("business_calendar")
    private BusinessCalendar businessCalendar;

    // for jackson
    @SuppressWarnings("unused")
    private PeriodStrategy() {
    }

    public PeriodStrategy(Template template) {
        this.setTemplate(template);
        this.setName(template.name());
    }

    public PeriodStrategy(BusinessCalendar calendar, Template template) {
        this.setTemplate(template);
        this.setName(template.name());
        this.setBusinessCalendar(calendar);
    }

    public Template getTemplate() {
        return template;
    }

    public void setTemplate(Template template) {
        this.template = template;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStartTimeStr() {
        return startTimeStr;
    }

    public void setStartTimeStr(String startTimeStr) {
        this.startTimeStr = startTimeStr;
    }

    public BusinessCalendar getBusinessCalendar() {
        return businessCalendar;
    }

    public void setBusinessCalendar(BusinessCalendar businessCalendar) {
        this.businessCalendar = businessCalendar;
    }

    public enum Template {
        Date, Day, Week, Month, Quarter, Year;

        private static Map<String, Template> lookupMap;

        static {
            lookupMap = Stream.of(Template.values()).collect(Collectors.toMap(e -> e.name().toUpperCase(), e -> e));
        }

        public static Template fromName(String templateName) {
            if (StringUtils.isBlank(templateName)) {
                return null;
            }
            return lookupMap.get(templateName.toUpperCase());
        }
    }

}
