package eu.stratosphere;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat.ConfigBuilder;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactBoolean;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextDoubleParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextIntParser;
import eu.stratosphere.pact.common.type.base.parser.DecimalTextLongParser;
import eu.stratosphere.pact.common.type.base.parser.VarLengthStringParser;
import eu.stratosphere.scala.operators.CsvInputFormat;

/**
 * 
 * @author Robert Metzger
 */
public class FlexCmp implements PlanAssembler, PlanAssemblerDescription {
	
	public static class Count extends ReduceStub implements Serializable {
		private String name;
		public Count(String name) {
			super();
			this.name = name;
		}
		@Override
		public void reduce(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			int cnt = 0;
			while(records.hasNext()) {
				records.next();
				cnt++;
			}
			PactRecord o = new PactRecord(2);
			o.setField(0, new PactString(name));
			o.setField(1, new PactInteger(cnt));
			out.collect(o);
		}
	}
	
	public static class VerifyReduce extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		private Class typeClass;
		public VerifyReduce(Class typeClass) {
			this.typeClass = typeClass;
		}
		
		PactRecord fromTSV = new PactRecord();
		PactRecord fromJoin = new PactRecord();
		@Override
		public void reduce(Iterator<PactRecord> records,
				Collector<PactRecord> out) throws Exception {
			fromTSV.clear();
			fromJoin.clear();
			while(records.hasNext()) {
				PactRecord r = records.next();
			//	System.err.println("Got record with "+r.getNumFields()+" fields");
				if(r.getNumFields() == 2) {
					r.copyTo(fromTSV);
				} else if(r.getNumFields() == 4) {
					boolean wasValid = r.getField(3, PactBoolean.class).getValue();
					if(wasValid) {
						return;
					}
					r.copyTo(fromJoin);
				} else {
					throw new RuntimeException("Unexpected. recrods field count = "+r.getNumFields());
				}
			}
			if(fromTSV.getNumFields() == 0 ) {
				throw new RuntimeException("Impossible");
			}
			if(fromJoin.getNumFields() == 0) { // no record from join. we could not match.
			//	System.err.println("No match");
				if(fromTSV.getNumFields() != 2) throw new RuntimeException("Unexpected 2");
				if(typeClass == DecimalTextIntParser.class ){
					fromTSV.setField(1, new PactInteger(-1)); // 1 contains csv value.
				} else {
					fromTSV.setField(1, new PactDouble(-1.0));
				}
				out.collect(fromTSV);
				return;
			}
			if(fromJoin.getNumFields() == 4) {
			//	System.err.println("From join");
				if(fromTSV.getNumFields() != 2) throw new RuntimeException("Unexpected 4");
				// all is "well". we found a wrong record and validated a match
				out.collect(fromJoin);
				return;
			}
			throw new RuntimeException("We can not end up here!");
		}
	}
	
	public static class Match extends MatchStub implements Serializable{
		private static final long serialVersionUID = 1L;
		private Class typeClass;
		public Match(Class typeClass) {
			this.typeClass = typeClass;
		}

		/**
		 * 0: str key,
		 * 1,2: csv, tsv
		 * 3: match flag ;)
		 */
		PactRecord ret = new PactRecord(4);
		PactBoolean flag = new PactBoolean(false); // indicating if result is valid 
		
		@Override
		public void match(PactRecord value1, PactRecord value2,
				Collector<PactRecord> out) throws Exception {
			flag.set(false);
			
			if(typeClass == DecimalTextIntParser.class) {
				int csv = value1.getField(1, PactInteger.class).getValue();
				int tsv = value2.getField(1, PactInteger.class).getValue();
				if(csv != tsv) {
					ret.setField(0, value1.getField(0, PactString.class));
					ret.setField(1, new PactInteger(csv));
					ret.setField(2, new PactInteger(tsv));
					ret.setField(3, flag);
					out.collect(ret);
					return;
				}
			} else if(typeClass == DecimalTextDoubleParser.class) {
				double csv = value1.getField(1, PactDouble.class).getValue();
				double tsv = value2.getField(1, PactDouble.class).getValue();
				if(Math.abs(csv-tsv) > 1 ) { // less then 1 distance. 
					ret.setField(0, value1.getField(0, PactString.class));
					ret.setField(1, new PactDouble(csv));
					ret.setField(2, new PactDouble(tsv));
					ret.setField(3, flag);
					out.collect(ret);
					return;
				}
			} else {
				throw new RuntimeException("Unknown type");
			}
			
			ret.setField(0, value1.getField(0, PactString.class));
			flag.set(true);
			ret.setField(3, flag);
			out.collect(ret);
			return;
			
		}
	}
	@Override
	public String getDescription() {
		return "Usage: [numSubStasks] [type] [inputCSV] [fieldID] [inputTSV] [fieldID] [output]";
	}
	
	@Override
	public Plan getPlan(String... args) {

		if (args.length < 5) {
			throw new RuntimeException(getDescription());
		}
		// parse job parameters
		int noSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1); 	// numSubTasks
		String type = (args.length > 1? args[1] : "");							// type (int, double)
		String inputCSV = (args.length > 2? args[2] : "");						// input csv
		int csvFieldID = (args.length > 3 ? Integer.parseInt(args[3]) : 1);		// field id
		String inputTSV = (args.length > 4 ? args[4] : "");
		int tsvFieldID = (args.length > 5 ? Integer.parseInt(args[5]) : 1);	
		String output = (args.length > 6 ? args[6] : "");

		Class typeClass = null;
		if(type.equals("int")) {
			typeClass = DecimalTextIntParser.class;
		}
		if(type.equals("double")) {
			typeClass = DecimalTextDoubleParser.class;
		}
		if(typeClass == null) throw new IllegalArgumentException("Unknown type");
		
		FileDataSource csv = new FileDataSource(RecordInputFormat.class, inputCSV, "CSV");
		RecordInputFormat.configureRecordFormat(csv)
		.recordDelimiter('\n')
		.fieldDelimiter(',')
		.field(VarLengthStringParser.class, 0)
		.field(typeClass, csvFieldID);	
		
		FileDataSource tsv = new FileDataSource(RecordInputFormat.class, inputTSV, "TSV");
		RecordInputFormat.configureRecordFormat(tsv)
		.recordDelimiter('\n')
		.fieldDelimiter('\t')
		.field(VarLengthStringParser.class, 0)
		.field(typeClass, tsvFieldID);	
		
		ReduceContract countCsv = ReduceContract.builder(new Count("CSV Input Count")).input(csv).build();
		ReduceContract countTsv = ReduceContract.builder(new Count("TSV Input Count")).input(tsv).build();
		
		
		
		MatchContract join = MatchContract.builder(new Match(typeClass), PactString.class, 0, 0)
							.input1(csv).input2(tsv).name("Match on string key").build();
		// union join and tsv and count
		ReduceContract verify = ReduceContract.builder(new VerifyReduce(typeClass),PactString.class,0)
				.input(join,tsv).name("Verify reducer").build();
		
		FileDataSink actualResult = new FileDataSink(RecordOutputFormat.class, output, verify, "Write result");
		ConfigBuilder b = RecordOutputFormat.configureRecordFormat(actualResult).recordDelimiter('\n')
				.fieldDelimiter(',').lenient(true).field(PactString.class, 0);
		if(typeClass == DecimalTextIntParser.class) {
			b.field(PactInteger.class, 1);
			b.field(PactInteger.class, 2);
		}
		if(typeClass == DecimalTextDoubleParser.class) {
			b.field(PactDouble.class, 1);
			b.field(PactDouble.class, 2);
		}
		List<GenericDataSink> out = new ArrayList<GenericDataSink>(2);
		
		ReduceContract countWrong = ReduceContract.builder(new Count("Total wrong count")).input(verify).build();
		FileDataSink statsOut = new FileDataSink(RecordOutputFormat.class, output+"-stats", "Write stats");
		statsOut.addInput(countCsv);
		statsOut.addInput(countTsv);
		statsOut.addInput(countWrong);
		RecordOutputFormat.configureRecordFormat(statsOut).recordDelimiter('\n')
				.fieldDelimiter('\t').lenient(true).field(PactString.class, 0).field(PactInteger.class, 1);
		
		out.add(statsOut);
		out.add(actualResult);
		Plan plan = new Plan(out, "Generic Comparator");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}


	
	
	public static void main(String[] args) throws Exception {
		FlexCmp ic = new FlexCmp();
		final String PATH = "file:///home/robert/Projekte/t-labs/bdaw.usecases/risk/tuberlin/verify-results/";
		Plan toExecute = ic.getPlan("1","double",
				PATH + "csv", "1",
				PATH + "tsv", "1",
				PATH + "result");
		LocalExecutor l = new LocalExecutor();
		System.err.println("Starting");
		l.start();
		l.executePlan(toExecute);
		System.exit(0);
	}
}
