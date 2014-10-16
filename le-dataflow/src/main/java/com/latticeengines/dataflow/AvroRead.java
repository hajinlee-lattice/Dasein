package com.latticeengines.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import cascading.avro.AvroScheme;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepListener;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.filter.Not;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;

import com.latticeengines.common.exposed.util.AvroUtils;

public class AvroRead {

    public static void main(String[] args) throws Exception {
        String lead = "file:///tmp/Lead/Lead_07-10-2014.avro";
        String opportunity = "file:///tmp/Opportunity/Opportunity_07-10-2014.avro";
        String wcPath = "/tmp/EventTable";

        // Get the schema from a file
        Schema leadSchema = AvroUtils.getSchema(new Configuration(), new Path(lead));
        Schema opportunitySchema = AvroUtils.getSchema(new Configuration(), new Path(opportunity));

        Properties properties = new Properties();
        properties.put("mapred.job.queue.name", "Priority0.MapReduce.0");
        AppProps.setApplicationJarClass(properties, AvroRead.class);
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        //FlowConnector flowConnector = new LocalFlowConnector();

        Map<String, Tap> sources = new HashMap<>();
        Tap<?, ?, ?> leadTap = new Lfs(new AvroScheme(), lead);
        Tap<?, ?, ?> opportunityTap = new Lfs(new AvroScheme(), opportunity);
        Tap<?, ?, ?> sink = new Lfs(new TextDelimited(/*new Fields("Id", "AnnualRevenue")*/), wcPath, true);
        sources.put("lead", leadTap);
        sources.put("oppty", opportunityTap);

        List<String> fieldNames = new ArrayList<>();
        Set<String> seenFieldNames = new HashSet<>();

        for (Field field : leadSchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = name + "_1";
            }
            fieldNames.add(name);
            seenFieldNames.add(name);
        }

        for (Field field : opportunitySchema.getFields()) {
            String name = field.name();
            if (seenFieldNames.contains(name)) {
                name = name + "_1";
            }
            fieldNames.add(name);
            seenFieldNames.add(name);
        }
        String[] declaredFields = new String[fieldNames.size()];
        fieldNames.toArray(declaredFields);
        Pipe join = new CoGroup(new Pipe("lead"), //
                new Fields("ConvertedOpportunityId"), //
                new Pipe("oppty"), //
                new Fields("Id"), //
                new Fields(declaredFields), //
                new InnerJoin());
        
        join = new GroupBy(join, new Fields("Company"));
        join = new Every(join, new Fields("AnnualRevenue"), new MaxValue(new Fields("MaxRevenue")), Fields.ALL);
        join = new Every(join, new Fields("NumberOfEmployees"), new Sum(new Fields("TotalEmployees")), Fields.ALL);
        
        ExpressionFilter filter = new ExpressionFilter("$0 > 0.0", Double.TYPE);
        Not not = new Not(filter);
        join = new Each(join, new Fields("MaxRevenue"), not);

        FlowDef flowDef = FlowDef.flowDef().setName("wc") //
                .addSources(sources) //
                .addTailSink(join, sink);
        
        // write a DOT file and run the flow
        Flow<?> flow = flowConnector.connect(flowDef);
        flow.writeDOT("dot/wcr.dot");
        flow.addStepListener(new DefaultFlowStepListener());
        flow.complete();
    }
    
    @SuppressWarnings("rawtypes")
    static class DefaultFlowStepListener implements FlowStepListener {
        
        @Override
        public void onStepStarting(FlowStep flowStep) {
            System.out.println("Starting " + flowStep.getName());
        }

        @Override
        public void onStepStopping(FlowStep flowStep) {
            System.out.println("Stopping " + flowStep.getName());
        }

        @Override
        public void onStepRunning(FlowStep flowStep) {
            System.out.println("Running " + flowStep.getName());
        }

        @Override
        public void onStepCompleted(FlowStep flowStep) {
            System.out.println("Completed " + flowStep.getName());
        }

        @Override
        public boolean onStepThrowable(FlowStep flowStep, Throwable throwable) {
            throwable.printStackTrace();
            return false;
        }
        
    }

}
