package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.python.jline.internal.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.FileCheckStrategy;

import edu.emory.mathcs.backport.java.util.Collections;

@Component("ingestionVersionService")
public class IngestionVersionServiceImpl implements IngestionVersionService {
    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Override
    public List<String> getMostRecentVersionsFromHdfs(String ingestionName, int checkVersion) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestionName);
        List<String> versions = new ArrayList<String>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())) {
                List<String> fullPaths = HdfsUtils.getFilesForDir(yarnConfiguration,
                        ingestionDir.toString());
                if (!CollectionUtils.isEmpty(fullPaths)) {
                    for (String fullPath : fullPaths) {
                        if (!fullPath.contains(HdfsPathBuilder.VERSION_FILE)
                                && fullPath.startsWith(ingestionDir.toString())) {
                            versions.add(new Path(fullPath).getName());
                        } else if (!fullPath.contains(HdfsPathBuilder.VERSION_FILE)
                                && fullPath.contains(ingestionDir.toString())) {
                            versions.add(new Path(
                                    fullPath.substring(fullPath.indexOf(ingestionDir.toString())))
                                            .getName());
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to scan hdfs directory " + ingestionDir.toString());
        }
        List<String> mostRecentVersions = new ArrayList<String>();
        if (versions.size() <= checkVersion) {
            mostRecentVersions.addAll(versions);
        } else {
            Collections.sort(versions, Collections.reverseOrder());
            for (int i = 0; i < checkVersion; i++) {
                mostRecentVersions.add(versions.get(i));
            }
        }
        return mostRecentVersions;
    }

    @Override
    public String getFileNamePattern(String version, String fileNamePrefix, String fileNamePostfix,
            String fileExtension, String fileTimestamp) {
        String fileVersion = "";
        if (!StringUtils.isEmpty(fileTimestamp)) {
            DateFormat dateFormat = new SimpleDateFormat(HdfsPathBuilder.DATE_FORMAT_STRING);
            dateFormat.setTimeZone(TimeZone.getTimeZone(HdfsPathBuilder.UTC));
            Date timestamp = null;
            try {
                timestamp = dateFormat.parse(version);
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse version " + version);
            }
            DateFormat df = new SimpleDateFormat(fileTimestamp);
            df.setTimeZone(TimeZone.getTimeZone(HdfsPathBuilder.UTC));
            fileVersion = df.format(timestamp);
        }
        return fileNamePrefix + fileVersion + fileNamePostfix + fileExtension;
    }

    @Override
    public List<String> getFileNamesOfMostRecentVersions(List<String> fileNames, int checkVersion,
            FileCheckStrategy checkStrategy, String fileTimestamp) {
        if (checkStrategy == FileCheckStrategy.ALL) {
            return fileNames;
        }
        String timestampPattern = fileTimestamp.replace("d", "\\d").replace("y", "\\d").replace("M",
                "\\d");
        Log.info("TimestampPattern: " + timestampPattern);
        Pattern pattern = Pattern.compile(timestampPattern);
        List<String> result = new ArrayList<String>();
        for (String fileName : fileNames) {
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.find()) {
                String timestampStr = matcher.group();
                DateFormat df = new SimpleDateFormat(fileTimestamp);
                try {
                    Date timestamp = df.parse(timestampStr);
                    Calendar cal = Calendar.getInstance();
                    switch (checkStrategy) {
                        case DAY:
                            cal.add(Calendar.DATE, -checkVersion);
                            break;
                        case WEEK:
                            cal.add(Calendar.DATE, -checkVersion * 7
                                    - (cal.get(Calendar.DAY_OF_WEEK) - cal.getFirstDayOfWeek()));
                            break;
                        case MONTH:
                            cal.add(Calendar.MONTH, -checkVersion);
                            cal.set(Calendar.DATE, 1);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unknown file check strategy: " + checkStrategy.toString());
                    }
                    df = new SimpleDateFormat("yyyyMMdd");
                    Date cutOffTimestamp = df.parse(df.format(cal.getTime()));
                    if (timestamp.compareTo(cutOffTimestamp) >= 0) {
                        result.add(fileName);
                    }
                } catch (ParseException e) {
                    throw new RuntimeException("Failed to parse timestamp " + timestampStr);
                }
            }
        }
        return result;
    }
}