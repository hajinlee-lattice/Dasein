package org.apache.sqoop.mapreduce;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.mapred.AvroJob;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.sqoop.mapreduce.hcat.SqoopHCatUtilities;
import org.apache.sqoop.util.PerfCounters;
import org.apache.sqoop.validation.ValidationContext;
import org.apache.sqoop.validation.ValidationException;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.config.ConfigurationHelper;
import com.cloudera.sqoop.io.CodecMap;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.mapreduce.JobBase;
import com.cloudera.sqoop.orm.TableClassName;
import com.cloudera.sqoop.util.ImportException;

/**
 * Base class for running an import MapReduce job. Allows dependency injection,
 * etc, for easy customization of import job types.
 */
@SuppressWarnings("deprecation")
public class ImportJobBase extends JobBase {

    private ImportJobContext context;

    public static final Log LOG = LogFactory.getLog(ImportJobBase.class.getName());

    /**
     * Controls how java.math.BigDecimal values should be converted to Strings
     * If set to true (default) then will call toPlainString() method. If set to
     * false then will call toString() method.
     */
    public static final String PROPERTY_BIGDECIMAL_FORMAT = "sqoop.bigdecimal.format.string";
    public static final boolean PROPERTY_BIGDECIMAL_FORMAT_DEFAULT = true;

    public ImportJobBase() {
        this(null);
    }

    public ImportJobBase(final SqoopOptions opts) {
        this(opts, null, null, null, null);
    }

    @SuppressWarnings("rawtypes")
    public ImportJobBase(final SqoopOptions opts, final Class<? extends Mapper> mapperClass,
            final Class<? extends InputFormat> inputFormatClass, final Class<? extends OutputFormat> outputFormatClass,
            final ImportJobContext context) {
        super(opts, mapperClass, inputFormatClass, outputFormatClass);
        this.context = context;
    }

    /**
     * Configure the output format to use for the job.
     */
    @Override
    protected void configureOutputFormat(Job job, String tableName, String tableClassName)
            throws ClassNotFoundException, IOException {

        job.setOutputFormatClass(getOutputFormatClass());

        if (isHCatJob) {
            LOG.debug("Configuring output format for HCatalog  import job");
            SqoopHCatUtilities.configureImportOutputFormat(options, job, getContext().getConnManager(), tableName,
                    job.getConfiguration());
            return;
        }

        if (options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
            job.getConfiguration().set("mapred.output.value.class", tableClassName);
        }

        if (options.shouldUseCompression()) {
            FileOutputFormat.setCompressOutput(job, true);

            String codecName = options.getCompressionCodec();
            Class<? extends CompressionCodec> codecClass;
            if (codecName == null) {
                codecClass = GzipCodec.class;
            } else {
                Configuration conf = job.getConfiguration();
                codecClass = CodecMap.getCodec(codecName, conf).getClass();
            }
            FileOutputFormat.setOutputCompressorClass(job, codecClass);

            if (options.getFileLayout() == SqoopOptions.FileLayout.SequenceFile) {
                SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
            }

            // SQOOP-428: Avro expects not a fully qualified class name but a
            // "short"
            // name instead (e.g. "snappy") and it needs to be set in a custom
            // configuration option called "avro.output.codec".
            // The default codec is "deflate".
            if (options.getFileLayout() == SqoopOptions.FileLayout.AvroDataFile) {
                if (codecName != null) {
                    String shortName = CodecMap.getCodecShortNameByName(codecName, job.getConfiguration());
                    // Avro only knows about "deflate" and not "default"
                    if (shortName.equalsIgnoreCase("default")) {
                        shortName = "deflate";
                    }
                    job.getConfiguration().set(AvroJob.OUTPUT_CODEC, shortName);
                } else {
                    job.getConfiguration().set(AvroJob.OUTPUT_CODEC, DataFileConstants.DEFLATE_CODEC);
                }
            }
        }

        Path outputPath = context.getDestination();
        FileOutputFormat.setOutputPath(job, outputPath);
    }

    /**
     * Actually run the MapReduce job.
     */
    @Override
    protected boolean runJob(Job job) throws ClassNotFoundException, IOException, InterruptedException {

        PerfCounters perfCounters = new PerfCounters();
        perfCounters.startClock();

        boolean success = doSubmitJob(job);

        if (isHCatJob) {
            SqoopHCatUtilities.instance().invokeOutputCommitterForLocalMode(job);
        }

        perfCounters.stopClock();

        Counters jobCounters = job.getCounters();
        // If the job has been retired, these may be unavailable.
        if (null == jobCounters) {
            displayRetiredJobNotice(LOG);
        } else {
            perfCounters.addBytes(jobCounters.getGroup("FileSystemCounters").findCounter("HDFS_BYTES_WRITTEN")
                    .getValue());
            LOG.info("Transferred " + perfCounters.toString());
            long numRecords = ConfigurationHelper.getNumMapOutputRecords(job);
            LOG.info("Retrieved " + numRecords + " records.");
        }
        return success;
    }

    /**
     * Submit the Map Reduce Job.
     */
    protected boolean doSubmitJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        LOG.info("Using ledp ImportJobBase.");
        boolean doSync = job.getConfiguration().getBoolean("sqoop.sync", false);
        boolean success = true;
        if (doSync) {
            success = job.waitForCompletion(true);
        } else {
            job.submit();
        }
        JobID jobId = job.getJobID();
        String fileName = job.getConfiguration().get("sqoop.app.id.file.name");
        File file = new File(fileName);
        LOG.info(String.format("Application id file full path is %s.", file.getAbsolutePath()));
        FileUtils.write(file, jobId.toString());

        return success;
    }

    /**
     * Run an import job to read a table in to HDFS.
     *
     * @param tableName
     *            the database table to read; may be null if a free-form query
     *            is specified in the SqoopOptions, and the ImportJobBase
     *            subclass supports free-form queries.
     * @param ormJarFile
     *            the Jar file to insert into the dcache classpath. (may be
     *            null)
     * @param splitByCol
     *            the column of the database table to use to split the import
     * @param conf
     *            A fresh Hadoop Configuration to use to build an MR job.
     * @throws IOException
     *             if the job encountered an IO problem
     * @throws ImportException
     *             if the job failed unexpectedly or was misconfigured.
     */
    public void runImport(String tableName, String ormJarFile, String splitByCol, Configuration conf)
            throws IOException, ImportException {
        // Check if there are runtime error checks to do
        if (isHCatJob && options.isDirect() && !context.getConnManager().isDirectModeHCatSupported()) {
            throw new IOException("Direct import is not compatible with "
                    + "HCatalog operations using the connection manager "
                    + context.getConnManager().getClass().getName() + ". Please remove the parameter --direct");
        }
        if (options.getAccumuloTable() != null && options.isDirect()
                && !getContext().getConnManager().isDirectModeAccumuloSupported()) {
            throw new IOException("Direct mode is incompatible with "
                    + "Accumulo. Please remove the parameter --direct");
        }
        if (options.getHBaseTable() != null && options.isDirect()
                && !getContext().getConnManager().isDirectModeHBaseSupported()) {
            throw new IOException("Direct mode is incompatible with " + "HBase. Please remove the parameter --direct");
        }
        if (null != tableName) {
            LOG.info("Beginning import of " + tableName);
        } else {
            LOG.info("Beginning query import.");
        }
        String tableClassName = null;
        if (!getContext().getConnManager().isORMFacilitySelfManaged()) {
            tableClassName = new TableClassName(options).getClassForTable(tableName);
        }
        // For ORM self managed, we leave the tableClassName to null so that
        // we don't check for non-existing classes.

        loadJars(conf, ormJarFile, tableClassName);

        Job job = createJob(conf);
        try {
            // Set the external jar to use for the job.
            job.getConfiguration().set("mapred.jar", ormJarFile);
            if (options.getMapreduceJobName() != null) {
                job.setJobName(options.getMapreduceJobName());
            }

            propagateOptionsToJob(job);
            configureInputFormat(job, tableName, tableClassName, splitByCol);
            configureOutputFormat(job, tableName, tableClassName);
            configureMapper(job, tableName, tableClassName);
            configureNumTasks(job);
            cacheJars(job, getContext().getConnManager());

            jobSetup(job);
            setJob(job);
            boolean success = runJob(job);
            if (!success) {
                throw new ImportException("Import job failed!");
            }

            completeImport(job);

            if (options.isValidationEnabled()) {
                validateImport(tableName, conf, job);
            }
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        } catch (ClassNotFoundException cnfe) {
            throw new IOException(cnfe);
        } finally {
            unloadJars();
            jobTeardown(job);
        }
    }

    /**
     * Perform any operation that needs to be done post map/reduce job to
     * complete the import.
     */
    protected void completeImport(Job job) throws IOException, ImportException {
    }

    protected void validateImport(String tableName, Configuration conf, Job job) throws ImportException {
        LOG.debug("Validating imported data.");
        try {
            ValidationContext validationContext = new ValidationContext(getRowCountFromDB(context.getConnManager(),
                    tableName), // source
                    getRowCountFromHadoop(job)); // target

            doValidate(options, conf, validationContext);
        } catch (ValidationException e) {
            throw new ImportException("Error validating row counts", e);
        } catch (SQLException e) {
            throw new ImportException("Error retrieving DB source row count", e);
        } catch (IOException e) {
            throw new ImportException("Error retrieving target row count", e);
        } catch (InterruptedException e) {
            throw new ImportException("Error retrieving target row count", e);
        }
    }

    /**
     * Open-ended "setup" routine that is called after the job is configured but
     * just before it is submitted to MapReduce. Subclasses may override if
     * necessary.
     */
    protected void jobSetup(Job job) throws IOException, ImportException {
    }

    /**
     * Open-ended "teardown" routine that is called after the job is executed.
     * Subclasses may override if necessary.
     */
    protected void jobTeardown(Job job) throws IOException, ImportException {
    }

    protected ImportJobContext getContext() {
        return context;
    }
}