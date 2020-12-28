package com.latticeengines.testframework.exposed.utils;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.DomSerializer;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.TagNode;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.json.JSONArray;
import org.skyscreamer.jsonassert.JSONAssert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.latticeengines.testframework.exposed.service.CSVParserAction;

public class Util {
    private static final Logger log = LoggerFactory.getLogger(Util.class);
    private static final long DOWNLOAD_TIMEOUT = 1000 * 60 * 20;

    public static Element getFileElement(String fileName) throws Exception {
        SAXBuilder sb = new SAXBuilder();
        Document doc = sb.build(fileName);
        return doc.getRootElement();
    }

    public static void saveXmlFile(Document document, String fileName) throws Exception {
        XMLOutputter outp = new XMLOutputter(Format.getPrettyFormat());
        Format format = outp.getFormat();
        format.setEncoding("UTF-8");
        format.setExpandEmptyElements(true);
        outp.setFormat(format);
        outp.output(document, new FileOutputStream(fileName));
        log.debug("XML document generated");
    }

    public static void saveXmlFile(Element docElement, String fileName) throws Exception {
        saveXmlFile(new Document(docElement), fileName);
    }

    public static void saveStreamToLocal(InputStream inputStream, String localFilePath, boolean overWrite) {
        File localFile = new File(localFilePath);
        if (localFile.exists() && !overWrite) {
            System.out.println(localFilePath + "exists and overWrite is false, do nothing");
        } else {
            try {
                FileUtils.copyInputStreamToFile(inputStream, new File(localFilePath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void copyFile(String sourceFilename, String targetFilename, boolean append) throws IOException {
        FileInputStream fin = new FileInputStream(sourceFilename);
        copyFile(fin, targetFilename, append);
    }

    public static void copyFile(File sourceFile, String targetFilename, boolean append) throws IOException {
        FileInputStream fin = new FileInputStream(sourceFile);
        copyFile(fin, targetFilename, append);
    }

    public static void copyFile(File sourceFile, File targetFile, boolean append) throws IOException {
        FileInputStream fin = new FileInputStream(sourceFile);
        FileOutputStream fout = new FileOutputStream(targetFile);
        copyFile(fin, fout, append);
    }

    public static void copyFile(FileInputStream sourceFile, FileOutputStream targetFile, boolean append)
            throws IOException {
        FileInputStream fin = sourceFile;
        FileOutputStream fout = targetFile;
        byte[] buffer = new byte[512];

        int count = 0;

        do {
            count = fin.read(buffer);
            if (count != -1) {
                fout.write(buffer);
            }
        } while (count != -1);
        fin.close();
        fout.close();
    }

    public static void copyFile(FileInputStream sourceFile, String targetFilename, boolean append) throws IOException {
        FileInputStream fin = sourceFile;
        FileOutputStream fout = new FileOutputStream(targetFilename, append);
        copyFile(fin, fout, append);
    }

    public static String hex2Rgb(String colorStr) {
        Color color = new Color(Integer.valueOf(colorStr.substring(1, 3), 16),
                Integer.valueOf(colorStr.substring(3, 5), 16), Integer.valueOf(colorStr.substring(5, 7), 16));
        return String.format("rgb(%d, %d, %d)", color.getRed(), color.getGreen(), color.getBlue());
    }

    // public static void screenShotSave(String fileName, WebDriver driver) {
    // try {
    // File scrFile = ((TakesScreenshot) driver).getScreenshotAs(OutputType.FILE);
    // File targetFile = new File(fileName);
    // if (targetFile.exists()) {
    // targetFile.delete();
    // }
    // Util.copyFile(scrFile, fileName, false);
    // } catch (IOException e) {
    // e.printStackTrace();
    // }
    // }

    // public static void screenShotSaveWithTimestamp(WebDriver driver, boolean
    // forFailedCase, String testMethodName) {
    // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss-S");
    // String fileName = null;
    // if (StringUtils.isEmpty(testMethodName)) {
    // fileName = String.format(forFailedCase ? "target/Fail_%s.png" :
    // "target/Screenshot_%s.png",
    // format.format(new Date()));
    // } else {
    // fileName = String.format(forFailedCase ? "target/Fail_%s_%s.png" :
    // "target/Screenshot_%s_%s.png",
    // testMethodName, format.format(new Date()));
    // }
    // screenShotSave(fileName, driver);
    // log.debug("Taking screenshot: " + fileName);
    // }
    //
    // public static void screenShotSaveWithTimestamp(WebDriver driver, boolean
    // forFailedCase) {
    // screenShotSaveWithTimestamp(driver, forFailedCase, null);
    // }

    public static boolean tryWaitDownloadComplete(File file) {
        long start = System.currentTimeMillis();
        while (!file.exists()) {
            try {
                long elapsedTimeMillis = System.currentTimeMillis() - start;
                if (elapsedTimeMillis > DOWNLOAD_TIMEOUT) {
                    return false;
                }
                log.debug("Wait for downloading complete for file " + file.getName());
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        return file.length() > 0;
    }

    public static boolean tryWaitDownloadComplete(String downloadedFilePath) {
        return tryWaitDownloadComplete(new File(downloadedFilePath));
    }

    public static boolean tryWaitDownloadComplete(String downloadedFolderPath, String wildcardFileName) {
        File[] files;
        long start = System.currentTimeMillis();
        while ((files = getFilesByWildcardFilter(downloadedFolderPath, wildcardFileName)).length == 0) {
            long elapsedTimeMillis = System.currentTimeMillis() - start;
            if (elapsedTimeMillis > DOWNLOAD_TIMEOUT) {
                return false;
            }
            log.debug("No file filtered for folder [{}] with wildcard [{}]", downloadedFolderPath, wildcardFileName);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }
        }
        for (File file : files) {
            if (!tryWaitDownloadComplete(file)) {
                return false;
            }
        }
        return true;
    }

    public static void deleteDownloadedFile(String downloadedFolderPath, String wildcardFileName) {
        if (StringUtils.isEmpty(wildcardFileName)) {
            log.warn("The wildcardFileName is empty!!");
            return;
        }
        for (File file : getFilesByWildcardFilter(downloadedFolderPath, wildcardFileName)) {
            Path path = FileSystems.getDefault().getPath(file.getAbsolutePath());
            try {
                Files.deleteIfExists(path);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }
    }

    public static String getLatestModifiedFilePath(String downloadedFolderPath, String wildcardFileName) {
        File[] files = getFilesByWildcardFilter(downloadedFolderPath, wildcardFileName);
        if (files.length == 0) {
            return null;
        }

        Arrays.sort(files, (o1, o2) -> Long.compare(o2.lastModified(), o1.lastModified()));
        return files[0].getAbsolutePath();
    }

    public static File[] getFilesByWildcardFilter(String dir, String filter) {
        File dirFile = new File(dir);
        FileFilter fileFilter = new WildcardFileFilter(filter);
        return dirFile.listFiles(fileFilter);
    }

    public static List<String> getAllFieldNamesFromCSV(String filePath, String... excludedFieldNames) {
        File testDataFile = getCurrentThreadResource(filePath);
        CSVParser csvParser = null;
        List<String> ret = null;
        try {
            assert testDataFile != null;
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            ret = new ArrayList<>(csvParser.getHeaderMap().keySet());
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        if (excludedFieldNames != null) {
            for (String excludedFieldName : excludedFieldNames) {
                ret.remove(excludedFieldName);
            }
        }

        return ret;
    }

    public static Map<String, String> getColumnNameCategoryMapFromModelMetadataFile(String filePath) {
        return getColumnNameCategoryMapFromModelMetadataFile(filePath, null, null);
    }

    public static Map<String, String> getColumnNameCategoryMapFromModelMetadataFile(String filePath,
            String filterCategory, String filterTag) {
        File file = new File(filePath);
        HashMap<String, String> columnNameAndCategoryMap = new HashMap<>();
        JsonObject root;
        try {
            root = new JsonParser().parse(new FileReader(file)).getAsJsonObject();
            JsonArray elements = root.getAsJsonArray("Metadata");
            for (JsonElement element : elements) {
                JsonObject innerElement = element.getAsJsonObject();
                String category = null;
                for (JsonElement extension : innerElement.getAsJsonArray("Extensions")) {
                    if (extension.getAsJsonObject().get("Key").getAsString().equalsIgnoreCase("Category")
                            && !extension.getAsJsonObject().get("Value").isJsonNull()) {
                        category = extension.getAsJsonObject().get("Value").getAsString();
                        break;
                    }
                }

                if (StringUtils.isNotEmpty(category)) {
                    if (StringUtils.isNotEmpty(filterCategory) && !filterCategory.equalsIgnoreCase(category)) {
                        continue;
                    }
                    if (StringUtils.isEmpty(filterTag)) {
                        columnNameAndCategoryMap.put(innerElement.get("ColumnName").getAsString(), category);
                    } else {
                        for (JsonElement tagElement : innerElement.getAsJsonArray("Tags")) {
                            if (tagElement.getAsString().equalsIgnoreCase(filterTag)) {
                                columnNameAndCategoryMap.put(innerElement.get("ColumnName").getAsString(), category);
                            }
                        }
                    }
                }
            }
        } catch (FileNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        return columnNameAndCategoryMap;
    }

    public static int getFileRecordNumber(String filePath) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            return csvParser.getRecords().size();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return 0;
    }

    public static Object parseFile(String filePath, CSVParserAction action) {
        return parseFile(new File(filePath), action);
    }

    public static Object parseFile(File file, CSVParserAction action) {
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            return action.parse(csvParser);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static Set<String> getHeaderOfCSV(String filePath) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            return csvParser.getHeaderMap().keySet();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static Object parseTestDataFile(String fileName, CSVParserAction action) {
        return parseFile(getCurrentThreadResource(fileName), action);
    }

    public static String parseHtmlByUseXPath(String inputInHtml, String xpathExpression) {
        XPathFactory factory = XPathFactory.newInstance();
        XPath xpath = factory.newXPath();
        try {
            TagNode tagNode = new HtmlCleaner().clean(inputInHtml);
            org.w3c.dom.Document doc = new DomSerializer(new CleanerProperties()).createDOM(tagNode);
            return xpath.evaluate(xpathExpression, doc, XPathConstants.STRING).toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static File getCurrentThreadResource(String filePath) {
        URL ret = Thread.currentThread().getContextClassLoader().getResource(filePath);
        try {
            assert ret != null;
            String decodedPath = URLDecoder.decode(ret.getPath(), "UTF-8");
            log.debug("Decode URL: " + decodedPath);
            return new File(decodedPath);
        } catch (UnsupportedEncodingException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public static String getQAEnd2EndResourceDir() {
        return getCurrentThreadResource("test-serviceapps-cdl-qa-end2end-context.xml").getParentFile().getAbsolutePath()
                + "/qaend2end";
    }

    public static File getFileInCurrentProject(String fileName) {
        return findFileInOneFolder(new File(System.getProperty("user.dir")), fileName);
    }

    private static final ThreadLocal<File> FILE_CACHE = new ThreadLocal<>();

    public static File findFileInOneFolder(File folder, String fileName) {
        FILE_CACHE.remove();
        findFileAndCache(folder, fileName);
        File result = FILE_CACHE.get();
        FILE_CACHE.remove();
        return result;
    }

    private static void findFileAndCache(File folder, String fileName) {
        folder.listFiles(s -> {
            if (s.isFile() && s.getName().equals(fileName)) {
                if (null == FILE_CACHE.get()) {
                    FILE_CACHE.set(s);
                }
                return true;
            } else if (s.isDirectory()) {
                findFileAndCache(s, fileName);
                return false;
            }
            return false;
        });
    }

    public static int stringToInt(String source) {
        int power = 0;
        int sum = 0;
        for (int i = source.length() - 1; i >= 0; --i) {
            try {
                int number = Integer.valueOf(String.valueOf(source.charAt(i)));
                sum += number * Math.pow(10, power);
                ++power;
            } catch (NumberFormatException ignored) {
            }
        }
        return sum;
    }

    public static String dateTimeMillisToString(long millis) {
        Date date = new Date(millis);
        SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy-HH:mm:ss");
        return dateFormat.format(date);
    }

    public static List<String> executeCommandline(final String command, @Nullable Predicate<String> filter)
            throws InterruptedException {
        List<String> result = new ArrayList<>();
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(command);
            process.waitFor();
            if (process.exitValue() != 0) {
                log.error("Command exit abnormally!");
                return result;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (InputStream inputStream = process.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
                LineNumberReader bufferedReader = new LineNumberReader(inputStreamReader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (null != filter) {
                    if (filter.test(line)) {
                        result.add(line);
                    }
                } else {
                    result.add(line);
                }
            }
        } catch (IOException e) {
            return null;
        }
        process.destroy();
        return result;
    }

    public static String pickOneCellValueFromCSV(String filePath, int lineNum, String fieldName) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            CSVRecord csvRecord = csvParser.getRecords().get(lineNum - 1);
            return csvRecord.get(fieldName);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static String pickOneCellValueFromCSV(String filePath, String fieldNameToPick, String fieldValueToPick,
            String fieldName) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            for (CSVRecord record : csvParser.getRecords()) {
                if (record.get(fieldNameToPick).equalsIgnoreCase(fieldValueToPick)) {
                    return record.get(fieldName);
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        throw new RuntimeException(String.format("Fail to find field \"%s\", with value \"%s\", in file \"%s\"",
                fieldNameToPick, fieldValueToPick, filePath));
    }

    public static List<String> pickOneColumnValuesFromCSV(String filePath, String fieldNameToPick) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            List<String> results = new ArrayList<>();
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            for (CSVRecord record : csvParser.getRecords()) {
                results.add(record.get(fieldNameToPick));
            }
            return results;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static boolean hasColumnInCSV(String filePath, String fieldName) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            return csvParser.getHeaderMap().containsKey(fieldName);
        } catch (IOException io) {
            throw new RuntimeException(io);
        }
    }

    public static boolean hasValueInCSV(String filePath, String fieldName) {
        File testDataFile = new File(filePath);
        CSVParser csvParser = null;
        try {
            csvParser = CSVParser.parse(testDataFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            for (CSVRecord record : csvParser.getRecords()) {
                if (StringUtils.isNotBlank(record.get(fieldName))) {
                    return true;
                }
            }
            return false;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            try {
                if (null != csvParser) {
                    csvParser.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static boolean hasValueForOneColumnFromCSV(String filePath, String fieldName) {
        boolean hasValue = false;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = null;
            int indexOffset = findFieldNameIndex(reader.readLine().split(","), fieldName);

            while ((line = reader.readLine()) != null) {
                String[] items = line.split(",");
                if (items.length < indexOffset) {
                    continue;
                } else {
                    if (items[indexOffset].isEmpty()) {
                        continue;
                    } else {
                        hasValue = true;
                        break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hasValue;
    }

    public static int countValueForOneColumnFromCSV(String filePath, String fieldName) {
        int hasValue = 0;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line = null;
            int indexOffset = findFieldNameIndex(reader.readLine().split(","), fieldName);

            while ((line = reader.readLine()) != null) {
                String[] items = line.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
                if (items.length <= indexOffset) {
                    continue;
                } else {
                    if (items[indexOffset].replaceAll("\"", "").isEmpty()) {
                        continue;
                    } else {
                        hasValue++;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hasValue;
    }

    private static int findFieldNameIndex(String[] items, String fieldName) {
        int indexOffset = 0;
        for (int i = items.length - 1; i >= 0; --i) {
            if ((fieldName).equals(items[i])) {
                indexOffset = i;
                break;
            }
        }
        return indexOffset;
    }

    public static boolean validateDateFormat(String format, String dateToVerify) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        formatter.setLenient(false);
        try {
            formatter.format(formatter.parse(dateToVerify));
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    public static void assertResult(String expectedResultJsonFile, JSONArray actualResult) {
        try {
            String jsonStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(expectedResultJsonFile),
                    "UTF-8");
            JSONAssert.assertEquals(jsonStr, actualResult, false);
        } catch (Exception e) {
            throw new AssertionError("Test result assertion failed.", e);
        }
    }

}
