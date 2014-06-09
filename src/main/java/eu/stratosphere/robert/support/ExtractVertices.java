package eu.stratosphere.robert.support;


import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.core.fs.FileSystem.WriteMode;
import eu.stratosphere.util.Collector;

public class ExtractVertices {

	 static final String TAB_DELIM = "\t" ;
	 static final String COMMA_DELIM = "," ;
	 static final String COMMA_STR = "comma" ;
	 static final String TAB_STR = "tab";
	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage:[Degree of parallelism],[edge input-path],[out-put]");
			return;
		}
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0])
				: 1);
		final String inputfilepath = (args.length > 1 ? args[1] : "");
		final String outputfilepath = (args.length > 2 ? args[2] : "");
	     String fieldDelimiter = " ";
	        if(args.length>3){
	        	fieldDelimiter = (args[3]);
	        }
	        if(COMMA_DELIM.equalsIgnoreCase(fieldDelimiter) || COMMA_STR.equalsIgnoreCase(fieldDelimiter)){
	        	fieldDelimiter = COMMA_DELIM;
	        } 
	        else if(TAB_DELIM.equalsIgnoreCase(fieldDelimiter) || TAB_STR.equalsIgnoreCase(fieldDelimiter)){
	        	fieldDelimiter = TAB_DELIM;
	        } else {
	        	fieldDelimiter = " "; 
	        }
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		env.setDegreeOfParallelism(numSubTasks);
		
		DataSet<String> text = env.readTextFile(inputfilepath);
		DataSet<Long> result =  text.map(new TextMapper(fieldDelimiter)).groupBy(0).reduceGroup(new Reducer());
		result.writeAsText(outputfilepath, WriteMode.OVERWRITE);
		env.execute();
		
	}

	
	public static final class TextMapper extends MapFunction<String,Tuple1<Long>>{
		private static final long serialVersionUID = 1L;
		private String delim ;
		public TextMapper(String delim){
			this.delim = delim;
		}
		@Override
		public Tuple1<Long> map(String value) throws Exception {
			
			String[] array = value.split(this.delim);
			Tuple1<Long> emit = new Tuple1<Long>();
			emit.f0 = Long.parseLong(array[0]);
			return emit;
		}
	}

	@eu.stratosphere.api.java.functions.GroupReduceFunction.Combinable
	public static final class Reducer extends GroupReduceFunction<Tuple1<Long>,Long>{
		private static final long serialVersionUID = 1L;
		@Override
		public void reduce(Iterator<Tuple1<Long>> values,
				Collector<Long> out) throws Exception {
			Long srcKey = values.next().f0;
			out.collect(srcKey);
		}
		
		@Override
        public void combine(Iterator<Tuple1<Long>> values, Collector<Tuple1<Long>> out) {
            out.collect(values.next());
        }
		
	}
	
	
}

