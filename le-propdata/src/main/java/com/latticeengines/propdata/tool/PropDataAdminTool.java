package com.latticeengines.propdata.tool;

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
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.util.DateRange;
import com.latticeengines.propdata.collection.util.LoggingUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

@SuppressWarnings("unused")
public class PropDataAdminTool {

    private static final String NS_COMMAND = "command";
    private static final String NS_SOURCE = "source";
    private static final String NS_START_DATE = "startDate";
    private static final String NS_END_DATE = "endDate";
    private static final String NS_SPLIT_MODE = "splitMode";
    private static final String NS_PERIOD_LENGTH = "periodLength";
    private static final String NS_NUM_PERIODS = "numPeriods";

    private static final String MODE_NUM = "number";
    private static final String MODE_LEN = "length";

    private static final String NS_PIVOT_DATE = "pivotDate";
    private static final String NS_PIVOT_VERSION = "baseVersion";

    private static final String JOB_SUBMITTER = "CommandLineRunner";

    private static final Pattern datePattern = Pattern.compile("(19|20)\\d{2}-(0\\d|1[012])-([012]\\d|3[01])");
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private static final ArgumentParser parser = ArgumentParsers.newArgumentParser("archive");

    private Command command;
    private PropDataSource propDataSource;

    private DateRange fullDateRange;
    private List<DateRange> periods;

    private Date pivotDate;

    static {
        parser.description("PropData Admin Tool");

        parser.addArgument("-s", "--source")
                .dest(NS_SOURCE)
                .required(true)
                .type(String.class)
                .choices(PropDataSource.allNames())
                .help("collection source");

        Subparsers subparsers = parser.addSubparsers().help("valid commands").dest(NS_COMMAND);
        addArchiveArgs(subparsers.addParser(Command.ARCHIVE.getName()).help("archive a collection source"));
        addPivotArgs(subparsers.addParser(Command.PIVOT.getName()).help("pivot a base source"));
    }

    private static void addArchiveArgs(Subparser parser) {
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

    private static void addPivotArgs(Subparser parser) {
        parser.addArgument("-pd", "--pivot-date")
                .dest(NS_PIVOT_DATE)
                .required(false)
                .type(String.class)
                .help("pivot date in the format of yyyy-MM-dd. Default is current time.");

        parser.addArgument("-v", "--base-version")
                .dest(NS_PIVOT_VERSION)
                .required(false)
                .type(String.class)
                .help("the version of the base source to be pivoted");
    }

    public PropDataAdminTool(){ }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        command = Command.fromName(ns.getString(NS_COMMAND));
        propDataSource = PropDataSource.fromName(ns.getString(NS_SOURCE));

        switch (command) {
            case ARCHIVE:
                validateAndTransformArchiveArgs(ns);
                break;
            case PIVOT:
                validateAndTransformPivotArgs(ns);
                break;
            default:
        }

    }

    private void validateAndTransformArchiveArgs(Namespace ns) {
        Date startDate = toStartOfTheDay(parseDateInput(ns.getString(NS_START_DATE)));
        Date endDate = toEndOfTheDay(parseDateInput(ns.getString(NS_END_DATE)));
        fullDateRange = new DateRange(startDate, endDate);

        if (ns.getString(NS_SPLIT_MODE).equals(MODE_LEN)) {
            int length = ns.getInt(NS_PERIOD_LENGTH);
            periods = fullDateRange.splitByDaysPerPeriod(length);
        }

        if (ns.getString(NS_SPLIT_MODE).equals(MODE_NUM)) {
            int num = ns.getInt(NS_NUM_PERIODS);
            periods = fullDateRange.splitByNumOfPeriods(num);
        }
    }

    private void validateAndTransformPivotArgs(Namespace ns) {
        if (StringUtils.isNotEmpty(ns.getString(NS_PIVOT_DATE))) {
            pivotDate = parseDateInput(ns.getString(NS_PIVOT_DATE));
        } else {
            pivotDate = new Date(System.currentTimeMillis());
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
        PropDataAdminTool runner = new PropDataAdminTool();
        runner.run(args);
    }

    private void run(String[] args) throws Exception {
        try {
            Namespace ns = parser.parseArgs(args);

            validateArguments(ns);

            switch (command) {
                case ARCHIVE:
                    executeArchiveCommand(ns);
                    break;
                case PIVOT:
                    executePivotCommand();
                    break;
                default:
            }

            System.out.println("\n\n========================================\n");
            promptContinue();
            System.exit(0);

        } catch (ArgumentParserException|IllegalArgumentException e) {
            handleException(e);
        }
    }

    private void executePivotCommand() {
//        System.out.println("\n\n========================================");
//        System.out.println("Pivoting Collection Source: " + source.getName());
//        System.out.println("========================================\n");
//
//        System.out.println("Source to pivot: " + source.getName());
//        archiveJobService.pivotData(pivotDate, hdfsSourceEntityMgr.getCurrentVersion(source.getCollectionSource()));
    }

    private void executeArchiveCommand(Namespace ns) {
        ClassPathXmlApplicationContext ac = new ClassPathXmlApplicationContext("propdata-job-context.xml");
        ArchiveService archiveService = (ArchiveService) ac.getBean(propDataSource.getServiceBean());

        System.out.println("\n\n========================================");
        System.out.println("Archiving Collection Source: " + propDataSource.getName());
        System.out.println("========================================\n");
        System.out.println(fullDateRange);
        System.out.println("Source to archive: " + propDataSource.getName());
        executeArchiveByRanges(archiveService);
        
        ac.close();
    }

    private void executeArchiveByRanges(ArchiveService archiveService) {
        System.out.println("Full date range to archive: " + fullDateRange);
        System.out.println("Split into " + periods.size() + " periods: ");

        int digits = String.valueOf(periods.size()).length();
        int i = 0;
        for (DateRange period : periods) {
            System.out.println(String.format("  Period %" + digits + "d: %s", ++i, period.toString()));
        }

        promptContinue();

        System.out.println("Start archiving " + propDataSource.getName() + " ... ");
        long totalStartTime = System.currentTimeMillis();
        for (DateRange period : periods) {
            long startTime = System.currentTimeMillis();
            System.out.println("Archiving data for (" + (++i) + "/" + periods.size() + ") period " + period
                    + " (check propdata.log and progress table for detailed progress.) ...");
            System.out.println("");

            try {
                ArchiveProgress progress = archiveService.startNewProgress(period.getStartDate(), period.getEndDate(), JOB_SUBMITTER);
                progress = archiveService.importFromDB(progress);
                if (i == periods.size()) {
                    progress = archiveService.transformRawData(progress);
                    progress = archiveService.exportToDB(progress);
                }
                archiveService.finish(progress);
                System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime)
                        + " TotalDuration=" + LoggingUtils.durationSince(totalStartTime));
            } catch (Exception e) {
                System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime)
                        + " TotalDuration=" + LoggingUtils.durationSince(totalStartTime));
            }
        }
    }

    private void promptContinue() {
        // prompt for continue
        while (true) {
            System.out.print("Do you want continue? (Y/n) ");
            try (Scanner scanner = new Scanner(System.in)) {
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
    }

    enum PropDataSource {
        FEATURE("Feature", "featureArchiveService"),
        BUILTWITH("BuiltWith", "builtWithArchiveService");

        private static Map<String, PropDataSource> nameMap;

        private final String name;
        private final String serviceBean;

        PropDataSource(String name, String archiveJobBean) {
            this.name = name;
            this.serviceBean = archiveJobBean;

        }

        String getName() { return this.name; }
        String getServiceBean() { return this.serviceBean; }

        static {
            nameMap = new HashMap<>();
            for (PropDataSource propDataSource : PropDataSource.values()) {
                nameMap.put(propDataSource.getName(), propDataSource);
            }
        }

        static String[] allNames() {
            Set<String> names = nameMap.keySet();
            String[] nameArray = new String[names.size()];
            return names.toArray(nameArray);
        }

        static PropDataSource fromName(String name) {
            return nameMap.get(name);
        }

    }

    enum Command {
        ARCHIVE("archive"),
        PIVOT("pivot");

        private static Map<String, Command> nameMap;
        private final String name;

        Command(String name) {
            this.name = name;
        }

        String getName() { return this.name; }

        static {
            nameMap = new HashMap<>();
            for (Command cmd: Command.values()) {
                nameMap.put(cmd.getName(), cmd);
            }
        }

        static String[] allNames() {
            Set<String> names = nameMap.keySet();
            String[] nameArray = new String[names.size()];
            return names.toArray(nameArray);
        }

        static Command fromName(String name) {
            return nameMap.get(name);
        }

    }

    private void executeMatch(String uid) {
        System.out.println("Join");

    }
}
