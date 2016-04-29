package com.latticeengines.domain.exposed.propdata.ingestion;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.latticeengines.common.exposed.util.CipherUtils;

public class SftpProtocol extends Protocol {

    private String sftpHost;
    private Integer sftpPort;
    private String sftpUserName;
    private String sftpPasswordEncrypted;
    private String sftpDir;

    @JsonProperty("SftpHost")
    public String getSftpHost() {
        return sftpHost;
    }

    @JsonProperty("SftpHost")
    public void setSftpHost(String sftpHost) {
        this.sftpHost = sftpHost;
    }

    @JsonProperty("SftpPort")
    public Integer getSftpPort() {
        return sftpPort;
    }

    @JsonProperty("SftpPort")
    public void setSftpPort(Integer sftpPort) {
        this.sftpPort = sftpPort;
    }

    @JsonProperty("SftpUsername")
    public String getSftpUserName() {
        return sftpUserName;
    }

    @JsonProperty("SftpUsername")
    public void setSftpUserName(String sftpUserName) {
        this.sftpUserName = sftpUserName;
    }

    @JsonProperty("SftpPassword")
    public String getSftpPasswordEncrypted() {
        return sftpPasswordEncrypted;
    }

    @JsonProperty("SftpPassword")
    public void setSftpPasswordEncrypted(String sftpPasswordEncrypted) {
        this.sftpPasswordEncrypted = sftpPasswordEncrypted;
    }

    @JsonProperty("SftpDir")
    public String getSftpDir() {
        return sftpDir;
    }

    @JsonProperty("SftpDir")
    public void setSftpDir(String sftpDir) {
        this.sftpDir = sftpDir;
    }

    @Override
    @JsonIgnore
    public List<String> getAllFiles() {
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(getSftpUserName(), getSftpHost(), getSftpPort());
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(CipherUtils.decrypt(getSftpPasswordEncrypted()));
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp sftpChannel = (ChannelSftp) channel;
            sftpChannel.cd("." + getSftpDir());
            Vector files = sftpChannel.ls(".");
            List<String> fileSources = new ArrayList<String>();
            for (int i = 0; i < files.size(); i++) {
                ChannelSftp.LsEntry file = (ChannelSftp.LsEntry) files.get(i);
                if (!file.getAttrs().isDir()) {
                    fileSources.add(file.getFilename());
                }
            }
            sftpChannel.exit();
            session.disconnect();
            return fileSources;
        } catch (JSchException | SftpException e) {
            throw new RuntimeException(e);
        }
    }
}
