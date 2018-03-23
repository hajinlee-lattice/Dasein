package com.latticeengines.domain.exposed.cdl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PeriodStrategy implements Serializable {

    private static final long serialVersionUID = 4453849238385668063L;
    public static PeriodStrategy CalendarWeek, CalendarMonth, CalendarQuarter, CalendarYear;
    public static final ImmutableList<PeriodStrategy> NATURAL_PERIODS = ImmutableList.of( //
            new PeriodStrategy(Template.Week), //
            new PeriodStrategy(Template.Month), //
            new PeriodStrategy(Template.Quarter), //
            new PeriodStrategy(Template.Year));

    static {
        CalendarWeek = new PeriodStrategy(Template.Week);
        CalendarMonth = new PeriodStrategy(Template.Month);
        CalendarQuarter = new PeriodStrategy(Template.Quarter);
        CalendarYear = new PeriodStrategy(Template.Year);
    }

    private PeriodStrategy() {
    }

    public PeriodStrategy(Template template) {
        this.setTemplate(template);
        this.setName(template.name());
    }

    @JsonProperty("template")
    private Template template;

    @JsonProperty("name")
    private String name;

    @JsonProperty("start_time")
    private String startTimeStr;

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

    public enum Template {
        Date, Day, Week, Month, Quarter, Year
    }

}
