package com.latticeengines.propdata.tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import org.apache.commons.lang.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.BulkArchiveService;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.SourceService;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.RawSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.DateRange;
import com.latticeengines.propdata.core.util.LoggingUtils;

@SuppressWarnings("unused")
public class PropDataAdminTool {

    private static final String NS_COMMAND = "command";
    private static final String NS_SOURCE = "source";
    private static final String NS_START_DATE = "startDate";
    private static final String NS_END_DATE = "endDate";
    private static final String NS_RAW_TYPE = "rawType";
    private static final String NS_SPLIT_MODE = "splitMode";
    private static final String NS_PERIOD_LENGTH = "periodLength";
    private static final String NS_NUM_PERIODS = "numPeriods";

    private static final String MODE_NUM = "number";
    private static final String MODE_LEN = "length";

    private static final String RAW_TYPE_BULK = "bulk";
    private static final String RAW_TYPE_COLLECTED = "collected";

    private static final String NS_PIVOT_DATE = "pivotDate";
    private static final String NS_PIVOT_VERSION = "baseVersion";

    private static final String NS_RETRY_UID = "retryUid";

    private static final String JOB_SUBMITTER = "CommandLineRunner";

    private static final Pattern datePattern = Pattern.compile("(19|20)\\d{2}-(0\\d|1[012])-([012]\\d|3[01])");
    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private static final ArgumentParser parser = ArgumentParsers.newArgumentParser("propdata");

    private Command command;
    private SourceService sourceService;
    private ProgressOrchestrator progressOrchestrator;
    private PropDataRawSource sourceToBeArchived;
    private PropDataDerivedSource sourceToBeRefreshed;

    private DateRange fullDateRange;
    private List<DateRange> periods;

    private Date pivotDate;
    private String baseVersions;

    private String uidToRetry;

    private ApplicationContext applicationContext;

    static {
        parser.description("PropData Admin Tool");

        Subparsers subparsers = parser.addSubparsers().help("valid commands").dest(NS_COMMAND);
        addArchiveArgs(subparsers.addParser(Command.ARCHIVE.getName()).help("archive a collection source"));
        addRefreshArgs(subparsers.addParser(Command.REFRESH.getName()).help("pivot a base source"));
        addRetryArgs(subparsers.addParser(Command.RETRY.getName()).help("retry a process"));
    }

    private static void addArchiveArgs(Subparser parser) {
        parser.addArgument("-s", "--source").dest(NS_SOURCE).required(true).type(String.class)
                .choices(PropDataRawSource.allNames()).help("source to archive");

        parser.addArgument("-t", "--type").dest(NS_RAW_TYPE).required(true).type(String.class)
                .choices(new String[] { RAW_TYPE_BULK, RAW_TYPE_COLLECTED }).help("type of the raw source.");

        parser.addArgument("-sd", "--start-date").dest(NS_START_DATE).required(false).type(String.class)
                .help("start date (inclusive) in yyyy-MM-dd, and after 1900-01-01. (for collected source)");

        parser.addArgument("-ed", "--end-date").dest(NS_END_DATE).required(false).type(String.class)
                .help("end date (exclusive) in yyyy-MM-dd, and after start date. (for collected source)");

        parser.addArgument("-m", "--split-mode").dest(NS_SPLIT_MODE).required(false).type(String.class)
                .choices(new String[] { MODE_LEN, MODE_NUM }).setDefault(MODE_LEN)
                .help("mode of splitting date ranges, default is [length]: "
                        + "length = by the length of one period in days; "
                        + "number = by the number of periods. (for collected source)");

        parser.addArgument("-l", "--period-length").dest(NS_PERIOD_LENGTH).required(false).type(Integer.class)
                .setDefault(7)
                .help("period lengths in days. required if the split mode is length. default is [7] days. (for collected source)");

        parser.addArgument("-n", "--num-periods").dest(NS_NUM_PERIODS).required(false).type(Integer.class).setDefault(1)
                .help("number of periods. required if the split mode is number. default is [1] period. (for collected source)");
    }

    private static void addRefreshArgs(Subparser parser) {
        parser.addArgument("-s", "--source").dest(NS_SOURCE).required(true).type(String.class)
                .choices(PropDataDerivedSource.allNames()).help("source to refresh");

        parser.addArgument("-pd", "--pivot-date").dest(NS_PIVOT_DATE).required(false).type(String.class)
                .help("pivot date in the format of yyyy-MM-dd. Default is current time.");

        parser.addArgument("-v", "--base-version").dest(NS_PIVOT_VERSION).required(false).type(String.class)
                .help("the version of the base sources to be pivoted, separated by | if multiple base sources");
    }

    private static void addRetryArgs(Subparser parser) {
        parser.addArgument("-u", "--uuid").dest(NS_RETRY_UID).required(true).type(String.class)
                .help("root operation uid");
    }

    public PropDataAdminTool() {
    }

    private void validateArguments(Namespace ns) {
        if (ns == null) {
            throw new IllegalArgumentException("Failed to parse input arguments.");
        }

        command = Command.fromName(ns.getString(NS_COMMAND));
        switch (command) {
        case ARCHIVE:
            validateAndTransformArchiveArgs(ns);
            break;
        case REFRESH:
            validateAndTransformRefreshArgs(ns);
            break;
        case RETRY:
            validateAndTransformRetryArgs(ns);
            break;
        default:
        }

    }

    private void validateAndTransformArchiveArgs(Namespace ns) {
        sourceToBeArchived = PropDataRawSource.fromName(ns.getString(NS_SOURCE));

        if (sourceToBeArchived.getSourceType().equalsIgnoreCase(RAW_TYPE_COLLECTED)) {
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
    }

    private void validateAndTransformRefreshArgs(Namespace ns) {
        sourceToBeRefreshed = PropDataDerivedSource.fromName(ns.getString(NS_SOURCE));

        String pDate = ns.getString(NS_PIVOT_DATE);
        if (StringUtils.isEmpty(pDate)) {
            pivotDate = new Date();
        } else {
            pivotDate = toStartOfTheDay(parseDateInput(pDate));
        }

        baseVersions = ns.getString(NS_PIVOT_VERSION);
    }

    private void validateAndTransformRetryArgs(Namespace ns) {
        uidToRetry = ns.getString(NS_RETRY_UID);

        Pattern pattern = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        Matcher matcher = pattern.matcher(uidToRetry.toLowerCase());
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid root operation uid: " + uidToRetry);
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
            case REFRESH:
                executeRefreshCommand();
                break;
            case RETRY:
                executeRetryCommand();
                break;
            default:
            }

            System.out.println("\n\n========================================\n");
            promptContinue();
            System.exit(0);

        } catch (ArgumentParserException | IllegalArgumentException e) {
            handleException(e);
        }
    }

    private void executeRetryCommand() {
        System.out.println("\n\n========================================");
        System.out.println("Retrying job: " + uidToRetry);
        System.out.println("========================================\n");

        loadApplicationContext();

        executeRetry(uidToRetry);
    }

    private void executeRefreshCommand() {
        System.out.println("\n\n========================================");
        System.out.println("Refreshing Source: " + sourceToBeRefreshed.getName());
        System.out.println("========================================\n");

        loadApplicationContext();
        DerivedSource source = (DerivedSource) sourceService.findBySourceName(sourceToBeRefreshed.getName());
        HdfsSourceEntityMgr hdfsSourceEntityMgr = (HdfsSourceEntityMgr) applicationContext
                .getBean("hdfsSourceEntityMgr");
        if (!(source instanceof MostRecentSource) && StringUtils.isEmpty(baseVersions)) {
            List<String> versions = new ArrayList<>();
            for (Source baseSource : source.getBaseSources()) {
                versions.add(hdfsSourceEntityMgr.getCurrentVersion(baseSource));
            }
            baseVersions = StringUtils.join(versions, "|");
        }

        RefreshService refreshService = progressOrchestrator.getRefreshService(source);

        executeRefresh(refreshService);
    }

    private void executeArchiveCommand(Namespace ns) {
        if (ns.getString(NS_RAW_TYPE).equalsIgnoreCase(RAW_TYPE_COLLECTED)) {
            System.out.println("\n\n========================================");
            System.out.println("Archiving Collection Source: " + sourceToBeArchived.getName());
            System.out.println("========================================\n");
            System.out.println(fullDateRange);

            System.out.println("Full date range to archive: " + fullDateRange);
            System.out.println("Split into " + periods.size() + " periods: ");

            int digits = String.valueOf(periods.size()).length();
            int i = 0;
            for (DateRange period : periods) {
                System.out.println(String.format("  Period %" + digits + "d: %s", ++i, period.toString()));
            }

            promptContinue();

            loadApplicationContext();
            RawSource source = (RawSource) sourceService.findBySourceName(sourceToBeArchived.getName());
            CollectedArchiveService collectedArchiveService = (CollectedArchiveService) progressOrchestrator
                    .getArchiveService(source);

            executeArchiveByRanges(collectedArchiveService);
        } else {
            System.out.println("\n\n========================================");
            System.out.println("Archiving Bulk Source: " + sourceToBeArchived.getName());
            System.out.println("========================================\n");

            loadApplicationContext();
            RawSource source = (RawSource) sourceService.findBySourceName(sourceToBeArchived.getName());
            BulkArchiveService archiveService = (BulkArchiveService) progressOrchestrator.getArchiveService(source);

            executeArchiveBulk(archiveService);
        }
    }

    private void executeArchiveByRanges(CollectedArchiveService collectedArchiveService) {
        System.out.println("Start archiving " + sourceToBeArchived.getName() + " ... ");
        long totalStartTime = System.currentTimeMillis();
        int i = 0;
        for (DateRange period : periods) {
            long startTime = System.currentTimeMillis();
            System.out.println("Archiving data for (" + (++i) + "/" + periods.size() + ") period " + period
                    + " (check propdata.log and progress table for detailed progress.) ...");
            System.out.println("");

            try {
                ArchiveProgress progress = collectedArchiveService.startNewProgress(period.getStartDate(),
                        period.getEndDate(), JOB_SUBMITTER);
                progress = collectedArchiveService.importFromDB(progress);
                collectedArchiveService.finish(progress);
                System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime) + " TotalDuration="
                        + LoggingUtils.durationSince(totalStartTime));
            } catch (Exception e) {
                System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime) + " TotalDuration="
                        + LoggingUtils.durationSince(totalStartTime) + " " + e.getMessage());
            }
        }
    }

    private void executeArchiveBulk(BulkArchiveService archiveService) {
        System.out.println("Start archiving " + sourceToBeArchived.getName() + " ... ");
        long startTime = System.currentTimeMillis();
        try {
            ArchiveProgress progress = archiveService.startNewProgress(JOB_SUBMITTER);
            progress = archiveService.importFromDB(progress);
            archiveService.finish(progress);
            System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime));
        } catch (Exception e) {
            System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime) + " " + e.getMessage());
        }
    }

    private void executeRefresh(RefreshService refreshService) {
        System.out.println("Start refreshing " + sourceToBeRefreshed.getName() + " ... ");
        long startTime = System.currentTimeMillis();
        try {
            RefreshProgress progress = refreshService.startNewProgress(pivotDate, baseVersions, JOB_SUBMITTER);
            progress = refreshService.transform(progress);
            progress = refreshService.exportToDB(progress);
            refreshService.finish(progress);
            System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime));
        } catch (Exception e) {
            System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime) + " " + e.getMessage());
        }
    }

    private void executeRetry(String uid) {
        System.out.println("Start retrying " + uid + " ... ");
        long startTime = System.currentTimeMillis();

        try {
            ArchiveProgressEntityMgr archiveProgressEntityMgr = (ArchiveProgressEntityMgr) applicationContext
                    .getBean("archiveProgressEntityMgr");
            ArchiveProgress archiveProgress = archiveProgressEntityMgr.findProgressByRootOperationUid(uid);
            if (archiveProgress != null) {
                executeRetryArchiveProgress(archiveProgress);
                System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime));
                return;
            }

            RefreshProgressEntityMgr refreshProgressEntityMgr = (RefreshProgressEntityMgr) applicationContext
                    .getBean("refreshProgressEntityMgr");
            RefreshProgress refreshProgress = refreshProgressEntityMgr.findProgressByRootOperationUid(uid);
            if (refreshProgress != null) {
                executeRetryRefreshProgress(refreshProgress);
                System.out.println("Done. Duration=" + LoggingUtils.durationSince(startTime));
                return;
            }

            System.out.println("Cannot find the progress with uid " + uid);
        } catch (Exception e) {
            System.out.println("Failed. Duration=" + LoggingUtils.durationSince(startTime) + " " + e.getMessage());
        }
    }

    private void executeRetryArchiveProgress(ArchiveProgress archiveProgress) {
        if (archiveProgress.getStatusBeforeFailed() == null) {
            System.out.println("ArchiveProgress " + archiveProgress.getRootOperationUID()
                    + " does not have a StatusBeforeFailed, so cannot retry.");
            return;
        }

        String sourceName = archiveProgress.getSourceName();
        PropDataRawSource source = PropDataRawSource.fromName(sourceName);
    }

    private void executeRetryRefreshProgress(RefreshProgress refreshProgress) {
        if (refreshProgress.getStatusBeforeFailed() == null) {
            System.out.println("RefreshProgress " + refreshProgress.getRootOperationUID()
                    + " does not have a StatusBeforeFailed, so cannot retry.");
            return;
        }
        String sourceName = refreshProgress.getSourceName();
        DerivedSource source = (DerivedSource) sourceService.findBySourceName(sourceName);
        RefreshService refreshService = progressOrchestrator.getRefreshService(source);

        refreshProgress.setStatus(ProgressStatus.FAILED);
        switch (refreshProgress.getStatusBeforeFailed()) {
        case NEW:
        case TRANSFORMING:
            refreshProgress = refreshService.transform(refreshProgress);
        case TRANSFORMED:
        case UPLOADING:
            refreshProgress = refreshService.exportToDB(refreshProgress);
            if (refreshProgress.getStatus().equals(ProgressStatus.FAILED)) {
                refreshProgress = refreshService.exportToDB(refreshProgress);
            }
        default:
            refreshService.finish(refreshProgress);
        }

    }

    private void loadApplicationContext() {
        applicationContext = new ClassPathXmlApplicationContext("propdata-tool-context.xml");
        sourceService = (SourceService) applicationContext.getBean("sourceService");
        progressOrchestrator = (ProgressOrchestrator) applicationContext.getBean("progressOrchestrator");
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

    enum PropDataRawSource {
        ALEXA("Alexa", RAW_TYPE_COLLECTED), ORB("OrbIntelligence", RAW_TYPE_COLLECTED), FEATURE("Feature",
                RAW_TYPE_COLLECTED), BUILTWITH("BuiltWith", RAW_TYPE_COLLECTED), HGDATARAW("HGDataRaw", RAW_TYPE_BULK);

        private static Map<String, PropDataRawSource> nameMap;

        private final String name;
        private final String sourceType;

        PropDataRawSource(String name, String sourceType) {
            this.name = name;
            this.sourceType = sourceType;
        }

        String getName() {
            return this.name;
        }

        String getSourceType() {
            return this.sourceType;
        }

        static {
            nameMap = new HashMap<>();
            for (PropDataRawSource propDataRawSource : PropDataRawSource.values()) {
                nameMap.put(propDataRawSource.getName(), propDataRawSource);
            }
        }

        static String[] allNames() {
            Set<String> names = nameMap.keySet();
            String[] nameArray = new String[names.size()];
            return names.toArray(nameArray);
        }

        static PropDataRawSource fromName(String name) {
            return nameMap.get(name);
        }

    }

    enum PropDataDerivedSource {
        ALEXA("AlexaMostRecent"), ORB("OrbIntelligenceMostRecent"), FEATURE("FeatureMostRecent"), FEATURE_PIVOTED(
                "FeaturePivoted"), BUILTWITH("BuiltWithMostRecent"), BUILTWITH_PIVOTED("BuiltWithPivoted"), HGDATA(
                        "HGData"), HGDATA_PIVOTED("HGDataPivoted");

        private static Map<String, PropDataDerivedSource> nameMap;

        private final String name;

        PropDataDerivedSource(String name) {
            this.name = name;
        }

        String getName() {
            return this.name;
        }

        static {
            nameMap = new HashMap<>();
            for (PropDataDerivedSource propDataDerivedSource : PropDataDerivedSource.values()) {
                nameMap.put(propDataDerivedSource.getName(), propDataDerivedSource);
            }
        }

        static String[] allNames() {
            Set<String> names = nameMap.keySet();
            String[] nameArray = new String[names.size()];
            return names.toArray(nameArray);
        }

        static PropDataDerivedSource fromName(String name) {
            return nameMap.get(name);
        }

    }

    enum Command {
        ARCHIVE("archive"), REFRESH("refresh"), RETRY("retry");

        private static Map<String, Command> nameMap;
        private final String name;

        Command(String name) {
            this.name = name;
        }

        String getName() {
            return this.name;
        }

        static {
            nameMap = new HashMap<>();
            for (Command cmd : Command.values()) {
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
