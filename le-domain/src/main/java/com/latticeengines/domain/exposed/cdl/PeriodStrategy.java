package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PeriodStrategy implements Serializable {

    private static final long serialVersionUID = 4453849238385668063L;
    public static PeriodStrategy CalendarWeek, CalendarMonth, CalendarQuarter, CalendarYear;

    static {
        CalendarWeek = new PeriodStrategy(Template.Week);
        CalendarMonth = new PeriodStrategy(Template.Month);
        CalendarQuarter = new PeriodStrategy(Template.Quarter);
        CalendarYear = new PeriodStrategy(Template.Year);
    }

    private PeriodStrategy(){}

    public PeriodStrategy(Template template) {
        PeriodStrategy periodStrategy = new PeriodStrategy();
        periodStrategy.setTemplate(template);
    }

    @JsonProperty("template")
    private Template template;

    @JsonProperty("stat_time")
    private String startTimeStr;

    public Template getTemplate() {
        return template;
    }

    public void setTemplate(Template template) {
        this.template = template;
    }

    public String getStartTimeStr() {
        return startTimeStr;
    }

    public void setStartTimeStr(String startTimeStr) {
        this.startTimeStr = startTimeStr;
    }

    public enum Template {
        Day, Week, Month, Quarter, Year, Custom
    }

}
