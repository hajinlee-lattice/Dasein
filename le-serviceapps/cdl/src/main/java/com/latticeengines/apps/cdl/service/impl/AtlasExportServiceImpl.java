package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AtlasExportEntityMgr;
import com.latticeengines.apps.cdl.service.AtlasExportService;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

@Component("atlasExportService")
public class AtlasExportServiceImpl implements AtlasExportService {

    private static Logger log = LoggerFactory.getLogger(AtlasExportServiceImpl.class);

    @Inject
    private AtlasExportEntityMgr atlasExportEntityMgr;

    @Override
    public AtlasExport createAtlasExport(String customerSpace, AtlasExportType exportType) {
        return atlasExportEntityMgr.createAtlasExport(exportType);
    }

    @Override
    public void addFileToSystemPath(String customerSpace, String uuid, String fileName) {
        AtlasExport atlasExport = atlasExportEntityMgr.findByUuid(uuid);
        if (atlasExport == null) {
            log.error("Cannot find Atlas Export Object with uuid: " + uuid);
            throw new IllegalArgumentException("Cannot find Atlas Export Object with uuid: " + uuid);
        }
        List<String> fileList = atlasExport.getFilesUnderSystemPath();
        if (fileList == null) {
            fileList = new ArrayList<>();
        }
        if (!fileList.contains(fileName)) {
            fileList.add(fileName);
        }
        atlasExport.setFilesUnderSystemPath(fileList);
        atlasExportEntityMgr.update(atlasExport);
    }

    @Override
    public void addFileToDropFolder(String customerSpace, String uuid, String fileName) {
        AtlasExport atlasExport = atlasExportEntityMgr.findByUuid(uuid);
        if (atlasExport == null) {
            log.error("Cannot find Atlas Export Object with uuid: " + uuid);
            throw new IllegalArgumentException("Cannot find Atlas Export Object with uuid: " + uuid);
        }
        List<String> fileList = atlasExport.getFilesUnderDropFolder();
        if (fileList == null) {
            fileList = new ArrayList<>();
        }
        if (!fileList.contains(fileName)) {
            fileList.add(fileName);
        }
        atlasExport.setFilesUnderDropFolder(fileList);
        atlasExportEntityMgr.update(atlasExport);
    }

//    @Override
//    public void updateAtlasExportSystemPath(String customerSpace, String uuid, String systemPath) {
//        AtlasExport atlasExport = atlasExportEntityMgr.findByUuid(uuid);
//        if (atlasExport == null) {
//            log.error("Cannot find Atlas Export Object with uuid: " + uuid);
//            throw new IllegalArgumentException("Cannot find Atlas Export Object with uuid: " + uuid);
//        }
//        atlasExport.setSystemPath(systemPath);
//        atlasExportEntityMgr.update(atlasExport);
//    }
//
//    @Override
//    public void updateAtlasExportDropfolderpath(String customerSpace, String uuid, String dropfolderPath) {
//        AtlasExport atlasExport = atlasExportEntityMgr.findByUuid(uuid);
//        if (atlasExport == null) {
//            log.error("Cannot find Atlas Export Object with uuid: " + uuid);
//            throw new IllegalArgumentException("Cannot find Atlas Export Object with uuid: " + uuid);
//        }
//        atlasExport.setDropfolderPath(dropfolderPath);
//        atlasExportEntityMgr.update(atlasExport);
//    }

    @Override
    public AtlasExport getAtlasExport(String customerSpace, String uuid) {
        return atlasExportEntityMgr.findByUuid(uuid);
    }
}
