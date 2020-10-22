package com.latticeengines.apps.core.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.conditions.StringCondition;
import com.amazonaws.services.identitymanagement.model.AccessKey;
import com.amazonaws.services.identitymanagement.model.AccessKeyMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.apps.core.entitymgr.DropBoxEntityMgr;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.util.S3ImportMessageUtils;
import com.latticeengines.aws.iam.IAMService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.BitTransferUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

@Service("dropBoxService")
public class DropBoxServiceImpl implements DropBoxService {

    private static final Logger log = LoggerFactory.getLogger(DropBoxServiceImpl.class);

    private static final String DROP_FOLDER = "dropfolder";
    private static final String POLICY_NAME = "dropfolder";
    private static final String SLASH = "/";
    private static final String STAR = "*";
    private static final String WILD_CARD = "/*";
    private static final String ARN_PREFIX = "arn:aws:s3:::";
    // naming convention for S3 bucket policy statement
    private static final String PUT_POLICY_ID = "RequirementsOnPut";
    // Default Share folder
    private static final String PS_SHARE = "PS_SHARE";
    private static final String PS_SHARE_INBOX = "INBOX";
    private static final String PS_SHARE_OUTBOX = "OUTBOX";
    // Template prefix
    private static final String DEFAULTSYSTEM = "DefaultSystem";
    private static final String TEMPLATES = "Templates";
    // Export folder:
    private static final String EXPORT = "Export";

    @Inject
    private DropBoxEntityMgr dropBoxEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private IAMService iamService;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @Value("${aws.customer.s3.region}")
    private String region;

    @Value("${aws.customer.account.id}")
    private String customerAccountId;

    @Value("${aws.s3.data.stage.bucket}")
    private String externalSystemBucket;

    @Override
    public DropBox create() {
        DropBox dropbox = dropBoxEntityMgr.createDropBox(region);
        String prefix = toPrefix(dropbox);
        if (!s3Service.isNonEmptyDirectory(customersBucket, prefix)) {
            s3Service.createFolder(customersBucket, prefix);
        }
        return dropbox;
    }

    @Override
    public void delete() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox != null) {
            String prefix = toPrefix(dropbox);
            s3Service.cleanupDirectory(customersBucket, prefix);

            String dropBoxId = dropbox.getDropBox();
            String userName = "c-" + dropBoxId;
            revokeAccessToLatticeUser(userName);
            String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
            if (StringUtils.isBlank(policyDoc)) {
                iamService.deleteCustomerUser(userName);
            }
        }
        dropBoxEntityMgr.delete(dropbox);
    }

    @Override
    public String getDropBoxBucket() {
        return customersBucket;
    }

    private String getRegion() {
        return region;
    }

    @Override
    public String getDropBoxPrefix() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox == null) {
            return null;
        } else {
            return toPrefix(dropbox);
        }
    }

    @Override
    public Tenant getDropBoxOwner(String dropBox) {
        return dropBoxEntityMgr.getDropBoxOwner(dropBox);
    }

    @Override
    public void createTenantDefaultFolder(String customerSpace) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        String template = dropBoxPrefix + '/' + TEMPLATES;
        s3Service.createFolder(dropBoxBucket, template);
        String path = template + "/" + PS_SHARE;
        s3Service.createFolder(dropBoxBucket, path);
        String subPath = path + "/" + PS_SHARE_INBOX;
        s3Service.createFolder(dropBoxBucket, subPath);
        subPath = path + "/" + PS_SHARE_OUTBOX;
        s3Service.createFolder(dropBoxBucket, subPath);
        createFolderWithSystemName(customerSpace, dropBoxBucket, dropBoxPrefix, DEFAULTSYSTEM);
    }

    @Override
    public void createFolder(String customerSpace, String systemName, String objectName, String path) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();

        if (StringUtils.isNotEmpty(systemName)) {// new logic that every system all have five folder, can not be edit.
            createFolderWithSystemName(customerSpace, dropBoxBucket, dropBoxPrefix, systemName);
        } else {// the old logic without systemName
            s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, null, formatPath(objectName), null));

            if (StringUtils.isNotEmpty(path)) {
                String[] folderList = path.split("/");
                String needCreateFolder = "";
                for (String folder : folderList) {
                    if (StringUtils.isNotEmpty(folder)) {
                        needCreateFolder += "/" + folder;
                        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, systemName,
                                formatPath(objectName), formatPath(needCreateFolder)));
                    }
                }
            }
        }
    }

    @Override
    public void createSubFolder(String customerSpace, String systemName, String objectName, String path) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();

        if (StringUtils.isNotEmpty(systemName)) {// new logic that every system all have five folder, can not be edit.
            if (StringUtils.isBlank(objectName) && StringUtils.isBlank(path)) {
                createFolderWithSystemName(customerSpace, dropBoxBucket, dropBoxPrefix, systemName);
            } else {
                s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, systemName, objectName, path));
            }
        } else {// the old logic without systemName
            s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, null, formatPath(objectName), null));

            if (StringUtils.isNotEmpty(path)) {
                String[] folderList = path.split("/");
                String needCreateFolder = "";
                for (String folder : folderList) {
                    if (StringUtils.isNotEmpty(folder)) {
                        needCreateFolder += "/" + folder;
                        s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, systemName,
                                formatPath(objectName), formatPath(needCreateFolder)));
                    }
                }
            }
        }
    }

    @Override
    public List<String> getDropFoldersFromSystem(String customerSpace, String systemName) {
        List<String> allSubFolders = getDropFolders(customerSpace, null, null, null);
        List<String> subFoldersUnderSystem = new ArrayList<>();
        for (String subFolder : allSubFolders) {
            if (systemName.equals(S3PathBuilder.getSystemNameFromFeedType(subFolder))) {
                subFoldersUnderSystem.add(subFolder);
            }
        }
        return subFoldersUnderSystem;
    }

    @Override
    public List<String> getDropFolders(String customerSpace, String systemName, String objectName, String path) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        if (StringUtils.isEmpty(objectName) && StringUtils.isEmpty(path) && StringUtils.isEmpty(systemName)) {
            // there has two situation: 1. this is old path: dropfolder/%s/Templates;
            // 2.this is new path need list all dropFolders under systemName
            Set<String> allSubFolders = new HashSet<>();
            List<String> rootSubFolders = s3Service.listSubFolders(dropBoxBucket, dropBoxPrefix);
            log.info("rootSubFolders is " + rootSubFolders.toString());
            rootSubFolders.remove(PS_SHARE);
            if (CollectionUtils.isNotEmpty(rootSubFolders)) {
                for (String folderName : rootSubFolders) {
                    if (folderName.equals(TEMPLATES)) {
                        List<String> subFolders = s3Service.listSubFolders(dropBoxBucket,
                                getFullPath(dropBoxPrefix, null, null, null));
                        subFolders.remove(PS_SHARE);
                        allSubFolders.addAll(subFolders);
                    } else {
                        allSubFolders.addAll(formatFolderName(folderName, s3Service.listSubFolders(dropBoxBucket,
                                getFullPath(dropBoxPrefix, folderName, null, null))));
                    }
                }
                return new ArrayList<>(allSubFolders);
            }
        }
        return s3Service.listSubFolders(dropBoxBucket,
                getFullPath(dropBoxPrefix, systemName, formatPath(objectName), formatPath(path)));
    }

    @Override
    public boolean uploadFileToS3(String customerSpace, String key, String s3FileName, String hdfsPath) {
        try (InputStream inputStream = HdfsUtils.getInputStream(yarnConfiguration, hdfsPath)) {
            String dropBoxBucket = getDropBoxBucket();
            key = getValidkey(dropBoxBucket, key, s3FileName);
            s3Service.uploadInputStream(dropBoxBucket, key, inputStream, false);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private String getValidkey(String bucketname, String originalkey, String filename) {
        originalkey = PathUtils.formatKey(bucketname, originalkey);
        if (!originalkey.endsWith("/")) {
            originalkey += "/";
        }
        String dest_key = originalkey + filename;
        int suffix = 1;
        int dot = filename.lastIndexOf(".");
        String suffix_name = filename.substring(dot + 1);
        String file_name = filename.substring(0, dot);
        while (s3Service.objectExist(bucketname, dest_key)) {
            dest_key = originalkey + file_name + "_" + suffix + "." + suffix_name;
            suffix++;
        }
        return dest_key;
    }

    private String formatPath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
        }

        return path;
    }

    @Override
    public DropBoxSummary getDropBoxSummary() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox == null) {
            return null;
        } else {
            DropBoxSummary summary = new DropBoxSummary();
            summary.setBucket(customersBucket);
            summary.setDropBox(dropbox.getDropBox());
            if (dropbox.getAccessMode() != null) {
                summary.setAccessMode(dropbox.getAccessMode());
                if (DropBoxAccessMode.LatticeUser.equals(dropbox.getAccessMode())) {
                    summary.setLatticeUser(dropbox.getLatticeUser());
                    String userName = dropbox.getLatticeUser();
                    AccessKeyMetadata md = iamService.getCustomerKeyIfExists(userName);
                    if (md != null) {
                        summary.setAccessKeyId(md.getAccessKeyId());
                    }
                } else {
                    summary.setExternalAccount(dropbox.getExternalAccount());
                }
            }
            return summary;
        }
    }

    @Override
    public GrantDropBoxAccessResponse grantAccess(GrantDropBoxAccessRequest request) {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox == null) {
            log.info("Tenant " + MultiTenantContext.getShortTenantId() //
                    + " does not have a dropbox yet, create one.");
            dropbox = create();
        }
        revokeAccess();
        dropbox.setAccessMode(request.getAccessMode());
        GrantDropBoxAccessResponse response;
        switch (request.getAccessMode()) {
            case LatticeUser:
                response = grantAccessToLatticeUser(request.getExistingUser());
                dropbox.setLatticeUser(response.getLatticeUser());
                dropbox.setEncryptedSecretKey(response.getSecretKey());
                break;
            case ExternalAccount:
                response = grantAccessToExternalAccount(request.getExternalAccountId());
                dropbox.setExternalAccount(response.getExternalAccountId());
                break;
            default:
                throw new UnsupportedOperationException("Unknown access mode " + request.getAccessMode());
        }
        dropBoxEntityMgr.update(dropbox);
        return response;
    }

    public GrantDropBoxAccessResponse refreshAccessKey() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox == null) {
            throw new RuntimeException("Tenant " + MultiTenantContext.getShortTenantId() //
                    + " does not have a dropbox.");
        }
        String userName = dropbox.getLatticeUser();
        if (!DropBoxAccessMode.LatticeUser.equals(dropbox.getAccessMode()) || StringUtils.isBlank(userName)) {
            throw new RuntimeException("The access to the dropbox for " + MultiTenantContext.getShortTenantId() //
                    + " has not been granted to a lattice IAM user. Cannot refresh the access key.");
        }
        GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
        response.setDropBox(dropbox.getDropBox());
        response.setRegion(dropbox.getRegion());
        response.setBucket(customersBucket);
        response.setAccessMode(DropBoxAccessMode.LatticeUser);
        response.setLatticeUser(userName);
        AccessKey newKey = iamService.refreshCustomerKey(userName);
        response.setAccessKey(newKey.getAccessKeyId());
        response.setSecretKey(newKey.getSecretAccessKey());
        dropbox.setEncryptedSecretKey(newKey.getSecretAccessKey());
        dropBoxEntityMgr.update(dropbox);
        return response;
    }

    public GrantDropBoxAccessResponse getAccessKey() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox == null) {
            throw new RuntimeException("Tenant " + MultiTenantContext.getShortTenantId() //
                    + " does not have a dropbox.");
        } else {
            GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
            response.setDropBox(dropbox.getDropBox());
            response.setRegion(dropbox.getRegion());
            response.setBucket(customersBucket);
            if (dropbox.getAccessMode() != null) {
                response.setAccessMode(DropBoxAccessMode.LatticeUser);
                if (DropBoxAccessMode.LatticeUser.equals(dropbox.getAccessMode())) {
                    response.setLatticeUser(dropbox.getLatticeUser());
                    AccessKeyMetadata md = iamService.getCustomerKeyIfExists(dropbox.getLatticeUser());
                    response.setSecretKey(dropbox.getEncryptedSecretKey());
                    if (md != null) {
                        response.setAccessKey(md.getAccessKeyId());
                    }
                }
            }
            return response;
        }

    }

    @Override
    public void revokeAccess() {
        DropBox dropbox = dropBoxEntityMgr.getDropBox();
        if (dropbox != null && dropbox.getAccessMode() != null) {
            switch (dropbox.getAccessMode()) {
                case LatticeUser:
                    revokeAccessToLatticeUser(dropbox.getLatticeUser());
                    break;
                case ExternalAccount:
                    revokeDropBoxFromBucket(getDropBoxId(), dropbox.getExternalAccount());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown access mode " + dropbox.getAccessMode());
            }
            dropbox.setExternalAccount(null);
            dropbox.setLatticeUser(null);
            dropbox.setAccessMode(null);
            dropBoxEntityMgr.update(dropbox);
        }
    }

    @Override
    public List<FileProperty> getFileListForPath(String customerSpace, String relativePath, String filter) {
        final String delimiter = "/";
        String bucket = getDropBoxBucket();
        String dropbox = getDropBoxPrefix();
        relativePath = trimSlash(relativePath);
        String prefix = StringUtils.isBlank(relativePath) ? dropbox : dropbox + SLASH + relativePath;
        List<S3ObjectSummary> s3ObjectSummaries = s3Service.getFilesWithInfoForDir(bucket, prefix);
        List<FileProperty> fileList = new LinkedList<>();
        for (S3ObjectSummary summary : s3ObjectSummaries) {
            String fileName = summary.getKey();
            if (fileName.startsWith(prefix)) {
                fileName = fileName.replaceFirst(prefix, "");
            }
            fileName = PathUtils.formatPath(fileName);
            String fileType = PathUtils.getFileType(fileName);
            if (StringUtils.isNotEmpty(filter)) {
                if (fileName.contains("/") || fileType == null || !fileType.equalsIgnoreCase(filter)) {
                    continue;
                }
            }
            FileProperty fileProperty = new FileProperty();
            fileProperty.setFileName(fileName);
            fileProperty.setByteSize(summary.getSize());
            fileProperty.setFileSize(BitTransferUtils.formatSize(summary.getSize()));
            fileProperty.setFilePath(summary.getBucketName() + delimiter + summary.getKey());
            fileProperty.setLastModified(summary.getLastModified());
            if (StringUtils.isNotEmpty(fileType)) {
                fileType = fileType.toLowerCase();
            }
            fileProperty.setFileType(fileType);
            fileProperty.setDirectory(false);
            fileList.add(fileProperty);
        }
        List<String> subFolders = s3Service.listSubFolders(bucket, prefix);
        for (String subFolder : subFolders) {
            FileProperty fileProperty = new FileProperty();
            fileProperty.setFileName(subFolder);
            fileProperty.setFilePath(bucket + delimiter + prefix + delimiter + subFolder);
            fileProperty.setDirectory(true);
            fileList.add(fileProperty);
        }
        return fileList;
    }

    private static String trimSlash(String part) {
        if (StringUtils.isBlank(part)) {
            return part;
        }
        while (part.startsWith(SLASH)) {
            part = part.substring(1);
        }
        while (part.endsWith(SLASH)) {
            part = part.substring(0, part.length() - 1);
        }
        return part;
    }

    @Override
    public String getExportPath(String customerSpace, AtlasExportType exportType, String datePrefix,
            String optionalId) {
        String bucket = getDropBoxBucket();
        String prefix = getDropBoxPrefix();
        String exportPath = prefix + '/' + EXPORT + '/';
        if (!s3Service.objectExist(bucket, exportPath)) {
            s3Service.createFolder(bucket, exportPath);
        }
        exportPath += exportType.getPathFriendlyName() + '/';
        if (!s3Service.objectExist(bucket, exportPath)) {
            s3Service.createFolder(bucket, exportPath);
        }
        if (StringUtils.isNotEmpty(optionalId)) {
            exportPath += optionalId + '/';
            if (!s3Service.objectExist(bucket, exportPath)) {
                s3Service.createFolder(bucket, exportPath);
            }
        }
        exportPath += datePrefix + '/';
        if (!s3Service.objectExist(bucket, exportPath)) {
            s3Service.createFolder(bucket, exportPath);
        }
        return exportPath;
    }

    @Override
    public void createFolderUnderDropFolder(String path) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        if (StringUtils.isNotBlank(path)) {
            if (path.startsWith(SLASH)) {
                path = path.substring(1);
            }
        } else {
            return;
        }
        s3Service.createFolder(dropBoxBucket, dropBoxPrefix + SLASH + path);
    }

    @Override
    public void removeTemplatePath(String customerSpace, String feedType) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        String templatePath = dropBoxPrefix + SLASH + TEMPLATES + SLASH + feedType;
        if (s3Service.objectExist(dropBoxBucket, templatePath)) {
            s3Service.cleanupDirectory(dropBoxBucket, templatePath);
        }
    }

    @Override
    public void restoreTemplatePath(String customerSpace, String feedType) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        String templatePath = dropBoxPrefix + SLASH + TEMPLATES + SLASH + feedType;
        if (!s3Service.objectExist(dropBoxBucket, templatePath)) {
            s3Service.createFolder(dropBoxBucket, templatePath);
        }
    }

    @Override
    public void removeFolder(String path) {
        String dropBoxBucket = getDropBoxBucket();
        String dropBoxPrefix = getDropBoxPrefix();
        s3Service.cleanupDirectory(dropBoxBucket, dropBoxPrefix + SLASH + path);

    }

    private String getDropBoxId() {
        String prefix = getDropBoxPrefix();
        return prefix.substring(prefix.indexOf(SLASH) + 1);
    }

    private GrantDropBoxAccessResponse grantAccessToLatticeUser(String existingUser) {
        String bucket = getDropBoxBucket();
        String prefix = getDropBoxPrefix();
        String region = getRegion();
        String dropBoxId = prefix.substring(prefix.indexOf(SLASH) + 1);
        String userName = existingUser;
        if (StringUtils.isBlank(userName)) {
            userName = "c-" + dropBoxId;
            iamService.createCustomerUser(userName);
        }
        AccessKey accessKey = null;
        if (!iamService.hasCustomerKey(userName)) {
            accessKey = iamService.createCustomerKey(userName);
        }
        upsertDropBoxPolicy(userName, bucket, dropBoxId);
        GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
        response.setAccessMode(DropBoxAccessMode.LatticeUser);
        response.setLatticeUser(userName);
        if (accessKey != null) {
            response.setAccessKey(accessKey.getAccessKeyId());
            response.setSecretKey(accessKey.getSecretAccessKey());
            response.setBucket(bucket);
            response.setRegion(region);
            response.setDropBox(dropBoxId);
        }
        return response;
    }

    private void upsertDropBoxPolicy(String userName, String bucket, String dropBoxId) {
        String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
        Policy policy;
        if (StringUtils.isBlank(policyDoc)) {
            policy = newDropBoxPolicy(bucket, dropBoxId);
        } else {
            policy = Policy.fromJson(policyDoc);
            appendDropBox(policy, bucket, dropBoxId);
        }
        iamService.putUserPolicy(userName, POLICY_NAME, policy.toJson());
    }

    private Policy newDropBoxPolicy(String bucket, String dropBoxId) {
        String arnPrefix = ARN_PREFIX + bucket + SLASH + DROP_FOLDER + SLASH + dropBoxId;
        return new Policy().withStatements(//
                listDropBoxStatement(bucket, dropBoxId), //
                new Statement(Statement.Effect.Allow) //
                        .withId("Objects") //
                        .withActions(//
                                S3Actions.AbortMultipartUpload, //
                                S3Actions.GetObject, //
                                S3Actions.PutObject, //
                                S3Actions.DeleteObject, //
                                S3Actions.SetObjectAcl, //
                                () -> "s3:GetObjectTagging", //
                                () -> "s3:PutObjectTagging"
                        ) //
                        .withResources(new Resource(arnPrefix + WILD_CARD)) //
        );
    }

    private void appendDropBox(Policy policy, String bucket, String dropBoxId) {
        boolean hasListBucketStmt = false;
        String listStmtId = listDropBoxStmtId(dropBoxId);
        for (Statement stmt : policy.getStatements()) {
            if (listStmtId.equals(stmt.getId())) {
                hasListBucketStmt = true;
                break;
            }
        }
        policy.getStatements().forEach(stmt -> {
            if ("Objects".equals(stmt.getId())) {
                List<Resource> resourceList = stmt.getResources();
                Set<String> resources = resourceList.stream().map(Resource::getId).collect(Collectors.toSet());
                String rsc = ARN_PREFIX + bucket + SLASH + DROP_FOLDER + SLASH + dropBoxId + WILD_CARD;
                if (!resources.contains(rsc)) {
                    resourceList.add(new Resource(rsc));
                    resources.add(rsc);
                }
            }
        });
        if (!hasListBucketStmt) {
            policy.getStatements().add(listDropBoxStatement(bucket, dropBoxId));
        }
    }

    private Statement listDropBoxStatement(String bucket, String dropBoxId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(listDropBoxStmtId(dropBoxId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource(ARN_PREFIX + bucket)) //
                .withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROP_FOLDER + SLASH + dropBoxId + STAR //
                ));
    }

    private String listDropBoxStmtId(String dropBoxId) {
        return "List" + dropBoxId;
    }

    private void revokeAccessToLatticeUser(String userName) {
        String prefix = getDropBoxPrefix();
        String dropBoxId = prefix.substring(prefix.indexOf(SLASH) + 1);
        String policyDoc = iamService.getUserPolicy(userName, POLICY_NAME);
        if (StringUtils.isNotBlank(policyDoc)) {
            Policy policy = Policy.fromJson(policyDoc);
            removeDropBoxFromUser(policy, dropBoxId);
            if (CollectionUtils.isEmpty(policy.getStatements())) {
                String msg = "After revoking access to dropbox " + dropBoxId + " from user " + userName + ", " //
                        + POLICY_NAME + " policy becomes dummy, remove it";
                log.info(msg);
                iamService.deleteUserPolicy(userName, POLICY_NAME);
            } else {
                iamService.putUserPolicy(userName, POLICY_NAME, policy.toJson());
            }
        }
    }

    private void removeDropBoxFromUser(Policy policy, String dropboxId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    List<Resource> resourceList = stmt.getResources().stream() //
                            .filter(rsc -> !rsc.getId().contains(dropboxId))//
                            .collect(Collectors.toList());
                    stmt.setResources(resourceList);
                }) //
                .filter(stmt -> !stmt.getId().contains(dropboxId) //
                        && CollectionUtils.isNotEmpty(stmt.getResources())) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private GrantDropBoxAccessResponse grantAccessToExternalAccount(String accountId) {
        if (StringUtils.isBlank(accountId)) {
            throw new IllegalArgumentException("Must provide a valid account id");
        }
        String dropBoxId = getDropBoxId();
        log.info("Trying to grant access to dropbox " + dropBoxId + " to external account " + accountId +
                ". Please contact PLO for help!");

        GrantDropBoxAccessResponse response = new GrantDropBoxAccessResponse();
        response.setAccessMode(DropBoxAccessMode.ExternalAccount);
        response.setExternalAccountId(accountId);
        return response;
    }

    private Policy getCustomerPolicy(String dropBoxId, String accountId) {
        String bucketPolicy = s3Service.getBucketPolicy(customersBucket);
        List<Statement> statements = new ArrayList<>();
        Policy policy;
        if (StringUtils.isBlank(bucketPolicy)) {
            policy = new Policy();
        } else {
            policy = Policy.fromJson(bucketPolicy);
            revokeAccountFromDropBox(policy, dropBoxId, accountId);
        }
        boolean hasAccountStmt = false;
        if (CollectionUtils.isNotEmpty(policy.getStatements())) {
            for (Statement stmt : policy.getStatements()) {
                if (stmt.getId().equals(accountId)) {
                    insertAccountStatement(customersBucket, dropBoxId, stmt);
                    hasAccountStmt = true;
                } else if (PUT_POLICY_ID.equals(stmt.getId())) {
                    addAccountFromPutStatement(stmt, accountId);
                }
                statements.add(stmt);
            }
        }
        if (!hasAccountStmt) {
            statements.add(getAccountStatement(customersBucket, dropBoxId, accountId));
        }
        statements.add(getAccountListDropBoxStatement(customersBucket, dropBoxId, accountId));
        policy.setStatements(statements);
        return policy;
    }

    private Statement getAccountStatement(String bucketName, String dropBoxId, String accountId) {
        String arn = ARN_PREFIX + bucketName + "/dropbox/" + dropBoxId;
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId) //
                .withPrincipals(new Principal(accountId)) //
                .withActions(//
                        S3Actions.AbortMultipartUpload, //
                        S3Actions.GetObject, //
                        S3Actions.PutObject, //
                        S3Actions.DeleteObject, //
                        S3Actions.SetObjectAcl, //
                        () -> "s3:GetObjectTagging", //
                        () -> "s3:PutObjectTagging" //
                ) //
                .withResources(new Resource(arn + STAR));
    }

    private Statement getAccountListDropBoxStatement(String bucketName, String dropBoxId, String accountId) {
        return new Statement(Statement.Effect.Allow) //
                .withId(accountId + "_" + dropBoxId + "_list") //
                .withPrincipals(new Principal(accountId)) //
                .withActions(S3Actions.ListObjects) //
                .withResources(new Resource(ARN_PREFIX + bucketName)).withConditions(new StringCondition(//
                        StringCondition.StringComparisonType.StringLike, //
                        "s3:prefix", //
                        DROP_FOLDER + SLASH + dropBoxId + STAR //
                ));
    }

    private void revokeAccountFromDropBox(Policy policy, String dropBoxId, String accountId) {
        List<Statement> nonEmptyStmts = policy.getStatements().stream() //
                .peek(stmt -> {
                    if (accountId.equals(stmt.getId())) {
                        List<Resource> resourceList = stmt.getResources().stream() //
                                .filter(rsc -> !rsc.getId().contains(dropBoxId))//
                                .collect(Collectors.toList());
                        stmt.setResources(resourceList);
                    }
                }) //
                .filter(stmt -> {
                    boolean keep = true;
                    if (CollectionUtils.isEmpty(stmt.getResources())) {
                        keep = false;
                    } else if (stmt.getId().contains(accountId) && stmt.getId().contains(dropBoxId)) {
                        keep = false;
                    }
                    return keep;
                }) //
                .collect(Collectors.toList());
        policy.setStatements(nonEmptyStmts);
    }

    private void revokeDropBoxFromBucket(String dropBoxId, String accountId) {
        String bucketPolicy = s3Service.getBucketPolicy(customersBucket);
        if (StringUtils.isBlank(bucketPolicy)) {
            return;
        }
        log.info(String.format("Trying to revoke dropbox access %s for account %s. Please contact PLO for help!",
                dropBoxId, accountId));
    }

    private void insertAccountStatement(String bucketName, String dropBoxId, Statement statement) {
        String arn = ARN_PREFIX + bucketName + "/dropbox/" + dropBoxId;
        List<Resource> rscs = new ArrayList<>(statement.getResources());
        rscs.add(new Resource(arn));
        rscs.add(new Resource(arn + STAR));
        statement.setResources(rscs);
    }

    private void addAccountFromPutStatement(Statement statement, String accountId) {
        List<Principal> principals = statement.getPrincipals();
        boolean hasAccount = principals.stream().anyMatch(principal -> principal.getId().contains(accountId));
        if (!hasAccount) {
            principals.add(new Principal(accountId));
            statement.setPrincipals(principals);
        }
    }

    private void removeAccountFromPutStatement(Statement statement, String accountId) {
        List<Principal> principals = statement.getPrincipals();
        principals = principals.stream() //
                .filter(principal -> !principal.getId().contains(accountId)).collect(Collectors.toList());
        statement.setPrincipals(principals);
    }

    private String toPrefix(DropBox dropbox) {
        return DROP_FOLDER + SLASH + dropbox.getDropBox();
    }

    // for tests
    void setCustomersBucket(String customersBucket) {
        this.customersBucket = customersBucket;
    }

    private List<String> formatFolderName(String systemName, List<String> folders) {
        if (StringUtils.isEmpty(systemName)) {
            return folders;
        }
        Set<String> formatedFolders = new HashSet<>();
        for (String folderName : folders) {
            formatedFolders.add(S3ImportMessageUtils.formatFeedType(systemName, folderName));
        }
        return new ArrayList<>(formatedFolders);
    }

    private void createFolderWithSystemName(String customerSpace, String dropBoxBucket, String dropBoxPrefix,
                                            String systemName) {
        if (StringUtils.isEmpty(systemName)) {
            throw new IllegalArgumentException("systemName can not be null.");
        }
        S3ImportSystem s3ImportSystem = cdlProxy.getS3ImportSystem(customerSpace, systemName);
        if (s3ImportSystem == null) {
            throw new IllegalArgumentException("Cannot find import system with name: " + systemName);
        }
        Collection<EntityType> entityTypes = s3ImportSystem.getSystemType().getEntityTypes();
        List<String> defaultFolders = entityTypes.stream().map(EntityType::getDefaultFeedTypeName).collect(Collectors.toList());
        for (String folderName : defaultFolders) {
            s3Service.createFolder(dropBoxBucket, getFullPath(dropBoxPrefix, systemName, folderName, null));
        }
    }

    private String getFullPath(String dropBoxPrefix, String systemName, String objectName, String path) {
        String fullPath = dropBoxPrefix + "/" + TEMPLATES;
        String folderName = S3PathBuilder.getFolderName(systemName, objectName);
        if (StringUtils.isNotEmpty(folderName)) {
            fullPath += "/" + folderName;
            if (StringUtils.isNotEmpty(path)) {
                fullPath += "/" + path;
            }
        }
        return fullPath;
    }
}
