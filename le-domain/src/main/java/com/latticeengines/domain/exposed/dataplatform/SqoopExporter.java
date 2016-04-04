package com.latticeengines.domain.exposed.dataplatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public class SqoopExporter {

    private static List<String> defaultHadoopArgs = Arrays.asList(
            "-Dmapreduce.task.timeout=600000",
            "-Dmapreduce.job.running.map.limit=32"
    );

    private String table;
    private String sourceDir;
    private DbCreds dbCreds;
    private String queue;
    private String customer;
    private int numMappers;
    private String javaColumnTypeMappings;
    private List<String> exportColumns;
    private Configuration yarnConfiguration;
    private boolean sync;
    private List<String> hadoopArgs;
    private List<String> otherOptions;

    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    @JsonProperty("source_dir")
    public String getSourceDir() {
        return sourceDir;
    }

    @JsonProperty("source_dir")
    public void setSourceDir(String sourceDir) {
        this.sourceDir = sourceDir;
    }

    @JsonProperty("db_creds")
    public DbCreds getDbCreds() {
        return dbCreds;
    }

    @JsonProperty("db_creds")
    public void setDbCreds(DbCreds dbCreds) {
        this.dbCreds = dbCreds;
    }

    @JsonProperty("queue")
    public String getQueue() {
        return queue;
    }

    @JsonProperty("queue")
    public void setQueue(String queue) {
        this.queue = queue;
        boolean queueInArgs = false;
        String queueArg = "-Dmapreduce.job.queuename=" + this.queue;
        List<String> args = new ArrayList<>();
        if (this.hadoopArgs != null) {
            for (String arg : this.hadoopArgs) {
                if (arg.contains("-Dmapreduce.job.queuename")) {
                    queueInArgs = true;
                    args.add(queueArg);
                } else {
                    args.add(arg);
                }
            }
        }
        if (!queueInArgs) {
            args.add(queueArg);
        }
        setHadoopArgs(args);
    }

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("num_mappers")
    public int getNumMappers() {
        return numMappers;
    }

    @JsonProperty("num_mappers")
    public void setNumMappers(int numMappers) {
        this.numMappers = numMappers;
    }

    @JsonProperty("java_column_type_mappings")
    public String getJavaColumnTypeMappings() {
        return javaColumnTypeMappings;
    }

    @JsonProperty("java_column_type_mappings")
    public void setJavaColumnTypeMappings(String javaColumnTypeMappings) {
        this.javaColumnTypeMappings = javaColumnTypeMappings;
    }

    @JsonProperty("export_columns")
    public List<String> getExportColumns() {
        return exportColumns;
    }

    @JsonProperty("export_columns")
    public void setExportColumns(List<String> exportColumns) {
        this.exportColumns = exportColumns;
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    @JsonProperty("sync")
    public boolean isSync() {
        return sync;
    }

    @JsonProperty("sync")
    public void setSync(boolean sync) {
        this.sync = sync;
    }

    @JsonProperty("hadoop_args")
    public List<String> getHadoopArgs() {
        return hadoopArgs;
    }

    @JsonProperty("hadoop_args")
    public void setHadoopArgs(List<String> hadoopArgs) {
        this.hadoopArgs = hadoopArgs;
    }

    @JsonProperty("other_opts")
    public List<String> getOtherOptions() {
        return otherOptions;
    }

    @JsonProperty("other_opts")
    public void setOtherOptions(List<String> otherOptions) {
        this.otherOptions = otherOptions;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public static class Builder {

        private String table;
        private String sourceDir;
        private DbCreds dbCreds;
        private String queue;
        private String customer;
        private int numMappers = 0;
        private String javaColumnTypeMappings;
        private List<String> exportColumns = new ArrayList<>();
        private Configuration yarnConfiguration;
        private boolean sync = true;
        private List<String> hadoopArgs = new ArrayList<>();
        private List<String> otherOptions = new ArrayList<>();

        public SqoopExporter build() {
            SqoopExporter exporter =  new SqoopExporter();
            exporter.setTable(this.table);
            exporter.setSourceDir(this.sourceDir);
            exporter.setDbCreds(this.dbCreds);
            exporter.setQueue(this.queue);
            exporter.setCustomer(this.customer);
            exporter.setNumMappers(this.numMappers);
            exporter.setJavaColumnTypeMappings(this.javaColumnTypeMappings);
            exporter.setExportColumns(new ArrayList<>(this.exportColumns));
            exporter.setYarnConfiguration(this.yarnConfiguration);
            exporter.setSync(this.sync);

            Set<String> hadoopArgKeys = new HashSet<>();
            for (String arg: this.hadoopArgs) {
                if (arg.contains("=")) {
                    hadoopArgKeys.add(arg.substring(0, arg.indexOf("=")));
                }
            }

            for (String arg: defaultHadoopArgs) {
                String defaultKey = arg.substring(0, arg.indexOf("="));
                if (!hadoopArgKeys.contains(defaultKey)) {
                    this.addHadoopArg(arg);
                }
            }

            exporter.setHadoopArgs(new ArrayList<>(this.hadoopArgs));
            exporter.setOtherOptions(new ArrayList<>(this.otherOptions));

            return exporter;
        }

        public Builder setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder setSourceDir(String sourceDir) {
            this.sourceDir = sourceDir;
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

        public Builder setNumMappers(int numMappers) {
            this.numMappers = numMappers;
            return this;
        }

        public Builder setJavaColumnTypeMappings(String javaColumnTypeMappings) {
            this.javaColumnTypeMappings = javaColumnTypeMappings;
            return this;
        }

        public Builder setExportColumns(List<String> exportColumns) {
            this.exportColumns = exportColumns;
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
    }

}
