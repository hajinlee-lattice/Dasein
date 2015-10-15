package com.latticeengines.propdata.collection.job;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.propdata.collection.util.DateRange;
import com.latticeengines.propdata.collection.util.LoggingUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class ArchiveJobRunner {

    private static final String NS_SOURCE = "source";
    private static final String NS_START_DATE = "startDate";
    private static final String NS_END_DATE = "endDate";
    private static final String NS_SPLIT_MODE = "splitMode";
    private static final String NS_PERIOD_LENGTH = "periodLength";
    private static final String NS_NUM_PERIODS = "numPeriods";

    private static final String MODE_NUM = "number";
    private static final String MODE_LEN = "length";

    private static final String JOB_SUBMITTER = "CommandLineRunner";

    private static final Pattern datePattern = Pattern.compile("(19|20)\\d{2}-(0\\d|1[012])-([012]\\d|3[01])");
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private static final ArgumentParser parser = ArgumentParsers.newArgumentParser("archive");

    private DateRange fullDateRange;
    private CollectionSource source;
    private List<DateRange> periods;

    static {
        parser.description("Archive collection source in HDFS.");

        parser.addArgument("-s", "--source")
                .dest(NS_SOURCE)
                .required(true)
                .type(String.class)
                .choices(CollectionSource.allNames())
                .help("collection source");

        parser.addArgument("-sd", "--start-date")
                .dest(NS_START_DATE)
                .required(true)
                .type(String.class)
                .help("start date (inclusive) in yyyy-MM-dd, and after 1900-01-01.");

        parser.addArgument("-ed", "--end-date")
                .dest(NS_END_DATE)
                .required(true)
                .type(String.class)
                .help("end date (exclusive) in yyyy-MM-dd, and after start date.");

        parser.addArgument("-m", "--split-mode")
                .dest(NS_SPLIT_MODE)
                .required(false)
                .type(String.class)
                .choices(new String[]{ MODE_LEN, MODE_NUM })
                .setDefault(MODE_LEN)
                .help("mode of splitting date ranges, default is [length]: " +
                        "length = by the length of one period in days; " +
                        "number = by the number of periods.");

        parser.addArgument("-l", "--period-length")
                .dest(NS_PERIOD_LENGTH)
                .required(false)
                .type(Integer.class)
                .setDefault(7)
                .help("period lengths in days. required if the split mode is length. default is [7] days.");

        parser.addArgument("-n", "--num-periods")
                .dest(NS_NUM_PERIODS)
                .required(false)
                .type(Integer.class)
                .setDefault(1)
                .help("number of periods. required if the split mode is number. default is [1] period.");
    }

    public ArchiveJobRunner(){ }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        Date startDate = toStartOfTheDay(parseDateInput(ns.getString(NS_START_DATE)));
        Date endDate = toEndOfTheDay(parseDateInput(ns.getString(NS_END_DATE)));
        fullDateRange = new DateRange(startDate, endDate);
        System.out.println(fullDateRange);

        source = CollectionSource.fromName(ns.getString(NS_SOURCE));

        if (ns.getString(NS_SPLIT_MODE).equals(MODE_LEN)) {
            int length = ns.getInt(NS_PERIOD_LENGTH);
            periods = fullDateRange.splitByDaysPerPeriod(length);
        }

        if (ns.getString(NS_SPLIT_MODE).equals(MODE_NUM)) {
            int num = ns.getInt(NS_NUM_PERIODS);
            periods = fullDateRange.splitByNumOfPeriods(num);
        }
    }

    private Date parseDateInput(String input) {
        if (!datePattern.matcher(input).matches()) {
            throw new IllegalArgumentException("Incorrect format of date " + input);
        }
        try {
            return formatter.parse(input);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Cannot parse date " + input, e);
        }
    }

    private Date toStartOfTheDay(Date date) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    private Date toEndOfTheDay(Date date) {
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND, 999);
        return calendar.getTime();
    }

    private void handleException(Exception e) {
        if (e instanceof ArgumentParserException) {
            parser.handleError((ArgumentParserException) e);
        } else {
            parser.printUsage();
            System.out.println("error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        ArchiveJobRunner runner = new ArchiveJobRunner();
        runner.run(args);
    }

    private void run(String[] args) throws Exception {
        try {
            Namespace ns = parser.parseArgs(args);

            validateArguments(ns);

            System.out.println("\n\n========================================");
            System.out.println("Archiving Collection Source: " + source.getName());
            System.out.println("========================================\n");

            System.out.println("Source to archive: " + source.getName());
            System.out.println("Full date range to archive: " + fullDateRange);
            System.out.println("Split into " + periods.size() + " periods: ");

            int digits = String.valueOf(periods.size()).length();
            int i = 0;
            for (DateRange period : periods) {
                System.out.println(String.format("  Period %" + digits + "d: %s", ++i, period.toString()));
            }

            promptContinue();

            System.out.println("Start archiving " + source.getName() + " ... ");
            ApplicationContext ac = new ClassPathXmlApplicationContext("propdata-collection-context.xml");
            ArchiveJobService jobService = (ArchiveJobService) ac.getBean(source.getArchiveJobBean());
            jobService.setAutowiredArchiveService();
            jobService.setJobSubmitter(JOB_SUBMITTER);

            i = 0;
            long totalStartTime = System.currentTimeMillis();
            for (DateRange period : periods) {
                long startTime = System.currentTimeMillis();
                System.out.println("Archiving data for (" + (++i) + "/" + periods.size() + ") period " + period
                        + " (check propdata.log and progress table for detailed progress.) ...");
                System.out.println("");

                try {
                    jobService.archivePeriod(period);
                    System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime) +
                            " TotalDuration=" + LoggingUtils.durationSince(totalStartTime));
                } catch (Exception e) {
                    System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime) +
                            " TotalDuration=" + LoggingUtils.durationSince(totalStartTime));
                    continue;
                }
            }

            System.out.println("\n\n========================================\n");

            promptExit();

        } catch (ArgumentParserException|IllegalArgumentException e) {
            handleException(e);
        }
    }


    private void promptContinue() {
        // prompt for continue
        while (true) {
            System.out.print("Do you want continue? (Y/n) ");
            Scanner scanner = new Scanner(System.in);
            String answer = scanner.nextLine();
            if (answer.equalsIgnoreCase("Y") || StringUtils.isEmpty(answer)) {
                break;
            } else if (answer.equalsIgnoreCase("N")) {
                System.exit(0);
            } else {
                System.out.println("Unrecognized choice " + answer);
            }
        }
    }

    private void promptExit() throws IOException {
        System.out.println("Press enter to exit.");
        new Scanner(System.in).nextLine();
        System.exit(0);
    }


    enum CollectionSource {
        FEATURE("Feature", "featureArchiveJobService");

        private static Map<String, CollectionSource> nameMap;

        private String name;
        private String archiveJobBean;

        CollectionSource(String name, String archiveJobBean) {
            this.name = name;
            this.archiveJobBean = archiveJobBean;
        }

        String getName() { return this.name; }

        String getArchiveJobBean() { return this.archiveJobBean; }

        static {
            nameMap = new HashMap<>();
            for (CollectionSource source: CollectionSource.values()) {
                nameMap.put(source.getName(), source);
            }
        }

        static String[] allNames() {
            Set<String> names = nameMap.keySet();
            String[] nameArray = new String[names.size()];
            return names.toArray(nameArray);
        }

        static CollectionSource fromName(String name) {
            return nameMap.get(name);
        }

    }
}
