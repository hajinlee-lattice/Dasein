package com.latticeengines.dellebi.mbean;

import java.util.Arrays;
import java.util.Comparator;

import javax.annotation.Resource;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dellebi.entitymanager.DellEbiConfigEntityMgr;
import com.latticeengines.dellebi.service.FileFlowService;

import jcifs.smb.SmbFile;

@Component("smbFilesMBean")
public class SmbFilesMBean {

    static final Log log = LogFactory.getLog(SmbFilesMBean.class);

    private static final int TIMESTAMP_LENGTH = 6;

    private static final int YEAR_LENGTH = 4;

    private static final int TOTALL_PARSEDSTR_LENGTH = 20;

    public static final String INVALID_PARSEDSTR = "99999999999999999999";

    @Resource(name = "smbFileFlowService")
    protected FileFlowService smbFileFlowService;

    @Autowired
    protected DellEbiConfigEntityMgr dellEbiConfigEntityMgr;

    public String parseSmbFileName(String fileName) {

        String lastField;

        String strTmp = fileName.substring(0, fileName.lastIndexOf('.'));

        lastField = strTmp.substring(strTmp.lastIndexOf("_"), strTmp.length() - 1);
        int fieldLength = lastField.length();

        if (fieldLength == YEAR_LENGTH) {
            return parseFileNameAsYear(fileName);
        } else if (fieldLength == TIMESTAMP_LENGTH) {
            return parseFileNameAsGeneral(fileName);
        }

        return INVALID_PARSEDSTR;
    }

    public String parseFileNameAsYear(String fileName) {
        String timestamp = "000000";
        String sequence = "000";
        String priority = "000";
        String parsedStr;
        String strTmp = fileName.substring(0, fileName.lastIndexOf('.'));
        String date = strTmp.substring(strTmp.lastIndexOf("_") + 1, strTmp.length());
        date += "0000";
        strTmp = strTmp.substring(0, strTmp.lastIndexOf("_"));
        String actualSequence = strTmp.substring(strTmp.lastIndexOf("_") + 1, strTmp.length());
        if (actualSequence.length() > 3) {
            return INVALID_PARSEDSTR;
        }
        sequence = sequence.substring(0, 3 - actualSequence.length()) + actualSequence;
        String actualPriority = getPriorityByFileName(fileName);
        if (actualPriority.length() > 3) {
            return INVALID_PARSEDSTR;
        }
        priority = priority.substring(0, 3 - actualPriority.length()) + actualPriority;
        parsedStr = priority + date + timestamp + sequence;
        if (verifyParsedStr(parsedStr) == true) {
            return parsedStr;
        }
        return INVALID_PARSEDSTR;
    }

    public String parseFileNameAsGeneral(String fileName) {
        String timestamp;
        String date;
        String sequence = "000";
        String priority = "000";
        String parsedStr;
        String strTmp = fileName.substring(0, fileName.lastIndexOf('.'));

        timestamp = strTmp.substring(strTmp.lastIndexOf("_") + 1, strTmp.length());
        strTmp = strTmp.substring(0, strTmp.lastIndexOf("_"));
        date = strTmp.substring(strTmp.lastIndexOf("_") + 1, strTmp.length());
        strTmp = strTmp.substring(0, strTmp.lastIndexOf("_"));
        String actualSequence = strTmp.substring(strTmp.lastIndexOf("_") + 1, strTmp.length());
        if (actualSequence.length() > 3) {
            return INVALID_PARSEDSTR;
        }
        sequence = sequence.substring(0, 3 - actualSequence.length()) + actualSequence;
        String actualPriority = getPriorityByFileName(fileName);
        if (actualPriority.length() > 3) {
            return INVALID_PARSEDSTR;
        }
        priority = priority.substring(0, 3 - actualPriority.length()) + actualPriority;
        parsedStr = priority + date + timestamp + sequence;
        if (verifyParsedStr(parsedStr) == true) {
            return parsedStr;
        }
        return INVALID_PARSEDSTR;
    }

    public void sortSmbFiles(SmbFile[] smbFiles) {

        if (smbFiles == null) {
            log.warn("There is no files for sorting!");
            return;
        }

        Comparator<SmbFile> smbFilesComparator = new Comparator<SmbFile>() {
            @Override
            public int compare(SmbFile file1, SmbFile file2) {
                return ObjectUtils.compare(parseSmbFileName(file1.getName()), parseSmbFileName(file2.getName()));
            }
        };
        Arrays.sort(smbFiles, smbFilesComparator);
    }

    public String[] getSmbFileNamesArray(SmbFile[] smbFiles) {
        if (smbFiles == null) {
            return null;
        }
        String[] fileNamesArray = new String[smbFiles.length];
        int i = 0;
        for (SmbFile smbFile : smbFiles) {

            fileNamesArray[i++] = smbFile.getName();
        }

        return fileNamesArray;
    }

    private String getPriorityByFileName(String fileName) {
        String type = smbFileFlowService.getFileType(fileName).getType();
        return ObjectUtils.toString(dellEbiConfigEntityMgr.getPriority(type));
    }

    private Boolean verifyParsedStr(String parsedStr) {
        Boolean rc = true;
        if (parsedStr == null) {
            rc = false;
        }
        if (rc == true && parsedStr.length() != TOTALL_PARSEDSTR_LENGTH) {
            rc = false;
        }
        return rc;
    }
}
