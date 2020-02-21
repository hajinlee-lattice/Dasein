package com.latticeengines.domain.exposed.period;

import static java.time.temporal.ChronoUnit.DAYS;

import java.text.SimpleDateFormat;
import java.time.LocalDate;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NaturalDayPeriodBuilder extends StartTimeBasedPeriodBuilder implements PeriodBuilder {

    private static final Logger log = LoggerFactory.getLogger(NaturalDayPeriodBuilder.class);

    NaturalDayPeriodBuilder() {
        super();
    }

    @Override
    protected LocalDate getStartDate(int period) {
        return startDate.plusDays(period);
    }

    @Override
    protected LocalDate getEndDate(int period) {
        return startDate.plusDays(period);
    }

    @Override
    protected int getPeriodsBetweenDates(LocalDate start, LocalDate end) {
        long days = DAYS.between(start, end);
        return Long.valueOf(days).intValue();
    }

    public static void main(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        Option encrypt = new Option("encode", true, " - date to id");
        Option decrypt = new Option("decode", true, " - id to date");
        options.addOption(encrypt);
        options.addOption(decrypt);
        try {
            CommandLine cmd = parser.parse(options, args);
            NaturalDayPeriodBuilder builder = new NaturalDayPeriodBuilder();
            if (cmd.hasOption("encode")) {
                String dateStr = cmd.getOptionValue("encode");
                int dateId = builder.toPeriodId(dateStr);
                System.out.println("Date : " + dateStr);
                System.out.println("Date Id : " + dateId);
            } else if (cmd.hasOption("decode")) {
                int dateId = Integer.parseInt(cmd.getOptionValue("decode"));
                LocalDate dateStr = builder.getStartDate(dateId);
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                System.out.println("Date Id : " + dateId);
                System.out.println("Date  : " + format.format(dateStr));
            }
        } catch (Exception e) {
            log.error("Failed to parse command line.", e);
        }
    }

}
