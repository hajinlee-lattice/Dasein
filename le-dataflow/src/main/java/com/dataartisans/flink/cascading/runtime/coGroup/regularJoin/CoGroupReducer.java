/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink.cascading.runtime.coGroup.regularJoin;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowNode;
import cascading.flow.SliceCounters;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.element.ElementDuct;
import cascading.pipe.CoGroup;
import cascading.tuple.Tuple;
import com.dataartisans.flink.cascading.runtime.util.FlinkFlowProcess;
import com.dataartisans.flink.cascading.util.FlinkConfigConverter;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static cascading.util.LogUtil.logCounters;
import static cascading.util.LogUtil.logMemory;

@SuppressWarnings({"rawtypes"})
public class CoGroupReducer extends RichGroupReduceFunction<Tuple2<Tuple, Tuple[]>, Tuple> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8968637694593684076L;

	private static final Logger LOG = LoggerFactory.getLogger(CoGroupReducer.class);

	private FlowNode flowNode;
	private CoGroupReduceStreamGraph streamGraph;
	private CoGroupInGate groupSource;
	private FlinkFlowProcess currentProcess;

	private boolean calledPrepare;
	private long processBeginTime;

	public CoGroupReducer() {}

	public CoGroupReducer(FlowNode flowNode) {
		this.flowNode = flowNode;
	}

	@Override
	public void open(Configuration config) {

		this.calledPrepare = false;

		try {

			currentProcess = new FlinkFlowProcess(FlinkConfigConverter.toHadoopConfig(config), getRuntimeContext(), flowNode.getID());

			Set<FlowElement> sources = flowNode.getSourceElements();
			if(sources.size() != 1) {
				throw new RuntimeException("FlowNode for CoGroupReducer may only have a single source");
			}
			FlowElement sourceElement = sources.iterator().next();
			if(!(sourceElement instanceof CoGroup)) {
				throw new RuntimeException("Source of CoGroupReducer must be a CoGroup");
			}
			CoGroup source = (CoGroup)sourceElement;

			streamGraph = new CoGroupReduceStreamGraph( currentProcess, flowNode, source );
			groupSource = this.streamGraph.getGroupSource();

			for( Duct head : streamGraph.getHeads() ) {
				LOG.info("sourcing from: " + ((ElementDuct) head).getFlowElement());
			}

			for( Duct tail : streamGraph.getTails() ) {
				LOG.info("sinking to: " + ((ElementDuct) tail).getFlowElement());
			}
		}
		catch( Throwable throwable ) {

			if (throwable instanceof CascadingException) {
				throw (CascadingException) throwable;
			}

			throw new FlowException("internal error during CoGroupReducer configuration", throwable);
		}

	}

	@Override
	public void reduce(Iterable<Tuple2<Tuple, Tuple[]>> input, Collector<Tuple> output) throws Exception {

		this.streamGraph.setTupleCollector(output);

		if(! this.calledPrepare) {
			this.streamGraph.prepare();
			this.calledPrepare = true;

			this.groupSource.start(this.groupSource);

			this.processBeginTime = System.currentTimeMillis();
			currentProcess.increment( SliceCounters.Process_Begin_Time, processBeginTime );
		}

		try {
			this.groupSource.run(input.iterator());
		}
		catch( OutOfMemoryError error ) {
			throw error;
		}
		catch( Throwable throwable ) {
			if( throwable instanceof CascadingException ) {
				throw (CascadingException) throwable;
			}

			throw new FlowException( "internal error during CoGroupReducer execution", throwable );
		}

	}

	@Override
	public void close() {

		try {
			if( this.calledPrepare) {
				this.groupSource.complete(this.groupSource);

				this.streamGraph.cleanup();
			}
		}
		finally {
			if( currentProcess != null ) {
				long processEndTime = System.currentTimeMillis();
				currentProcess.increment( SliceCounters.Process_End_Time, processEndTime );
				currentProcess.increment( SliceCounters.Process_Duration, processEndTime - processBeginTime );
			}

			String message = "flow node id: " + flowNode.getID();
			logMemory( LOG, message + ", mem on close" );
			logCounters( LOG, message + ", counter:", currentProcess );
		}
	}

}
