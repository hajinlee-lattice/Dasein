package com.latticeengines.serviceflows.workflow.export;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Component("exportData")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportData extends BaseExportData<ExportStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportData.class);

    @Override
    public void execute() {
        log.info("Inside ExportData execute()");
        exportData();

        if (configuration.isExportMergedFile()){
            if (configuration.getExportFormat().equals(ExportFormat.CSV)){
                mergeCSVFile();
            }
        }
    }

    protected void mergeCSVFile(){
        String exportTargetPath = configuration.getExportTargetPath();
        int lastIndexOfSlash = exportTargetPath.lastIndexOf('/');
        String exportDir = exportTargetPath.substring(0,lastIndexOfSlash+1);
        String separatedFilePrefix = exportTargetPath.substring(lastIndexOfSlash+1);
        separatedFilePrefix = separatedFilePrefix.substring(0,separatedFilePrefix.length()-1);
        try {
            List<String> csvFiles = HdfsUtils.getFilesForDir(yarnConfiguration,exportDir,".*.csv$");
            String localInputDir = "separatedFile";
            File localDir = new File(localInputDir);
            if (!localDir.exists()){
                localDir.mkdir();
            }
            for (String filePath:csvFiles){
                HdfsUtils.copyHdfsToLocal(yarnConfiguration,filePath,localInputDir);
            }

            String targetCSVFileName = getExportedFilePath(exportTargetPath);
            targetCSVFileName = targetCSVFileName.substring(targetCSVFileName.lastIndexOf('/')+1);

            File localOutputCSV = new File(localInputDir,targetCSVFileName);
            boolean hasHeader = Boolean.FALSE;
            CSVWriter writer = new CSVWriter(new FileWriter(localOutputCSV), CSVWriter.DEFAULT_SEPARATOR,CSVWriter.NO_QUOTE_CHARACTER);
            for(File fileEntry:localDir.listFiles()){
                String fileName = fileEntry.getName();
                if (fileName.endsWith(".csv") && fileName.startsWith(separatedFilePrefix) && fileName.contains("part")){
                    CSVReader csvReader = new CSVReader(new FileReader(fileEntry));
                    List<String[]> records = csvReader.readAll();
                    if (!hasHeader){
                        writer.writeNext(records.get(0));
                        hasHeader = Boolean.TRUE;
                    }
                    writer.writeAll(records.subList(1,records.size()-1));
                    csvReader.close();
                }
            }
            writer.flush();
            writer.close();
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration,localOutputCSV.getPath(),exportDir);
            FileUtils.deleteDirectory(localDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Fix it: getExportedFilePath in MetadataSegmentExportServiceImpl
    protected String getExportedFilePath(String exportTargetPath){
        String filePath = exportTargetPath.substring(0,exportTargetPath.length()-1) + "_" + configuration.getMergedFileName();
        return filePath;
    }

    protected String getTableName() {
        String tableName = getStringValueFromContext(EXPORT_TABLE_NAME);
        if (tableName == null) {
            tableName = configuration.getTableName();
        }
        return tableName;
    }

    protected String getExportInputPath() {
        String inputPath = getStringValueFromContext(EXPORT_INPUT_PATH);
        return StringUtils.isNotBlank(inputPath) ? inputPath : null;
    }

    protected String getExportOutputPath() {
        String outputPath = getStringValueFromContext(EXPORT_OUTPUT_PATH);
        return StringUtils.isNotBlank(outputPath) ? outputPath : null;
    }

}
