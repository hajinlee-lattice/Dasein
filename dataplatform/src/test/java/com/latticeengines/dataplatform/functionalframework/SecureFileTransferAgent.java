package com.latticeengines.dataplatform.functionalframework;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs.FileObject;
import org.apache.commons.vfs.FileSystemOptions;
import org.apache.commons.vfs.FileType;
import org.apache.commons.vfs.Selectors;
import org.apache.commons.vfs.impl.StandardFileSystemManager;
import org.apache.commons.vfs.provider.sftp.SftpFileSystemConfigBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("secureFileTransferAgent")
public class SecureFileTransferAgent {

    private final static Log log = LogFactory
            .getLog(SecureFileTransferAgent.class);

    @Value("${dataplatform.yarn.resourcemanager.sftp.address}")
    private String serverAddress;

    @Value("${dataplatform.yarn.resourcemanager.sftp.userid}")
    private String userId;

    @Value("${dataplatform.yarn.resourcemanager.sftp.password}")
    private String password;

    public static enum FileTransferOption {
        UPLOAD, DOWNLOAD
    }

    public boolean fileTranser(String fileToFTP, String remoteFileToReplace,
            FileTransferOption option) {

        StandardFileSystemManager manager = new StandardFileSystemManager();

        try {

            // check if the file exists
            File file = new File(fileToFTP);
            if (!file.exists() && FileTransferOption.UPLOAD == option) {
                throw new RuntimeException("Error. Local file not found");
            }
            // Initializes the file manager
            manager.init();

            // Setup our SFTP configuration
            FileSystemOptions opts = new FileSystemOptions();
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(
                    opts, "no");
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(opts,
                    false);
            SftpFileSystemConfigBuilder.getInstance().setTimeout(opts, 10000);

            // Create the SFTP URI using the host name, userid, password, remote
            // path and file name
            String sftpUri = "sftp://" + userId + ":" + password + "@"
                    + serverAddress + remoteFileToReplace;

            // Create local file object
            FileObject localFile = manager.resolveFile(file.getAbsolutePath());

            // Create remote file object
            FileObject remoteFile = manager.resolveFile(sftpUri, opts);

            if (remoteFile.getType() == FileType.FOLDER) {
                log.error("cannot copy directories: " + remoteFile.getURL());
                return false;
            }

            // Copy local file to sftp server
            switch (option) {
            case UPLOAD:
                remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);
                log.info("File upload successful");
                break;
            case DOWNLOAD:
                localFile.copyFrom(remoteFile, Selectors.SELECT_SELF);
                log.info("File download successful");
                break;
            }
        } catch (Exception ex) {
            log.error(ex);
            return false;
        } finally {
            manager.close();
        }

        return true;
    }

}
