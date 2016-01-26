package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.modeling.DbCreds;

public class SqoopImporter {

    private String table;
    private String query;
    private String targetDir;
    private DbCreds dbCreds;
    private String queue;
    private String customer;
    private int numMappers;
    private String splitColumn;
    private List<String> columnsToInclude;
    private Configuration yarnConfiguration;
    private boolean sync;
    private Properties properties;
    private List<String> hadoopArgs;
    private List<String> otherOptions;
    private Mode mode;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }

    public DbCreds getDbCreds() {
        return dbCreds;
    }

    public void setDbCreds(DbCreds dbCreds) {
        this.dbCreds = dbCreds;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getCustomer() {
        return customer;
    }

    public void setCustomer(String customer) {
        this.customer = customer;
    }

    public int getNumMappers() {
        return numMappers;
    }

    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    public List<String> getColumnsToInclude() {
        return columnsToInclude;
    }

    public void setColumnsToInclude(List<String> columnsToInclude) {
        this.columnsToInclude = columnsToInclude;
    }

    public String getSplitColumn() {
        return splitColumn;
    }

    public void setSplitColumn(String splitColumn) {
        this.splitColumn = splitColumn;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }

    public List<String> getHadoopArgs() {
        return hadoopArgs;
    }

    public void setHadoopArgs(List<String> hadoopArgs) {
        this.hadoopArgs = hadoopArgs;
    }

    public List<String> getOtherOptions() {
        return otherOptions;
    }

    public void setOtherOptions(List<String> otherOptions) {
        this.otherOptions = otherOptions;
    }

    public Mode getMode() {
        return mode;
    }

    public void setMode(Mode mode) {
        this.mode = mode;
    }

    public static class Builder {

        private Mode mode = Mode.TABLE;
        private String table;
        private String query;
        private String targetDir;
        private DbCreds dbCreds;
        private String queue;
        private String customer;
        private String splitColumn;
        private List<String> columnsToInclude = new ArrayList<>();
        private Properties properties;
        private int numMappers = 0;
        private Configuration yarnConfiguration;
        private boolean sync = true;
        private List<String> hadoopArgs = new ArrayList<>();
        private List<String> otherOptions = new ArrayList<>(Arrays.asList(
                "--relaxed-isolation", "--as-avrodatafile", "--compress"
        ));

        public SqoopImporter build() {
            validate();
            SqoopImporter importer =  new SqoopImporter();
            importer.setMode(mode);
            importer.setTable(this.table);
            importer.setTargetDir(this.targetDir);
            importer.setDbCreds(this.dbCreds);
            importer.setQueue(this.queue);
            importer.setCustomer(this.customer);
            importer.setNumMappers(this.numMappers);
            importer.setColumnsToInclude(new ArrayList<>(this.columnsToInclude));
            importer.setSplitColumn(splitColumn);
            importer.setYarnConfiguration(this.yarnConfiguration);
            importer.setSync(this.sync);
            importer.setProperties(this.properties);

            this.addHadoopArg("-Dmapreduce.job.queuename=" + this.queue);
            importer.setHadoopArgs(new ArrayList<>(this.hadoopArgs));
            importer.setOtherOptions(new ArrayList<>(this.otherOptions));

            return importer;
        }

        public Builder setMode(Mode mode) {
            this.mode = mode;
            return this;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setQuery(String query) {
            this.query = query;
            return this;
        }

        public Builder setTargetDir(String targetDir) {
            this.targetDir = targetDir;
            return this;
        }

        public Builder setDbCreds(DbCreds dbCreds) {
            this.dbCreds = dbCreds;
            return this;
        }

        public Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder setCustomer(String customer) {
            this.customer = customer;
            return this;
        }

        public Builder setColumnsToInclude(List<String> columnsToInclude) {
            this.columnsToInclude = columnsToInclude;
            return this;
        }

        public Builder setSplitColumn(String splitColumn) {
            this.splitColumn = splitColumn;
            return this;
        }

        public Builder setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public Builder setNumMappers(int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder setYarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder setSync(boolean sync) {
            this.sync = sync;
            return this;
        }

        public Builder addHadoopArg(String hadoopArg) {
            this.hadoopArgs.add(hadoopArg);
            return this;
        }

        public Builder addExtraOption(String option) {
            this.otherOptions.add(option);
            return this;
        }

        private void validate() {
            if (Mode.TABLE.equals(this.mode) && StringUtils.isEmpty(this.table)) {
                throw new IllegalStateException("Table name not provided when importing in TABLE mode.");
            }

            if (Mode.QUERY.equals(this.mode) && StringUtils.isEmpty(this.query)) {
                throw new IllegalStateException("Query not provided when importing in TABLE mode.");
            }

            if (StringUtils.isEmpty(splitColumn) && this.numMappers > 1) {
                throw new IllegalStateException("Split column is not specified while requesting more than 1 mappers.");
            }

        }
    }

    public enum Mode { TABLE, QUERY }

}
