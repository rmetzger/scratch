package eu.stratosphere;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CrossContract;
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
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.CrossStub;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactBoolean;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * 
 * @author Robert Metzger
 */
public class FlexCmpCross implements PlanAssembler, PlanAssemblerDescription {
	
	public static class Identity extends MapStub {
		@Override
		public void map(PactRecord record, Collector<PactRecord> out)
				throws Exception {
			out.collect(record);
		}
	}
	
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

	enum GroupResult {
		BOTH_UNEQUAL,
		ONLY_CSV,
		ONLY_TSV
	}
	public static class CoGroup extends CoGroupStub {

		
		private static final long serialVersionUID = 1L;
		private Class typeClass;
		public CoGroup(Class typeClass) {
			this.typeClass = typeClass;
		}

		
		@Override
		public void coGroup(Iterator<PactRecord> csvIt,
				Iterator<PactRecord> tsvIt, Collector<PactRecord> out)
				throws Exception {
			PactRecord ret = new PactRecord();
			PactInteger flag = new PactInteger();
			List<PactRecord> csvs = new ArrayList<PactRecord>();
			List<PactRecord> tsvs = new ArrayList<PactRecord>();
			while(csvIt.hasNext()) {
				csvs.add(csvIt.next().createCopy());
			}
			while(tsvIt.hasNext()) {
				tsvs.add(tsvIt.next().createCopy());
			}
			Iterator<PactRecord> cIt = csvs.iterator();
			Iterator<PactRecord> tIt = tsvs.iterator();
			while(cIt.hasNext()) {
				PactRecord c = cIt.next();
				while(tIt.hasNext()) {
					PactRecord t = tIt.next();
					boolean match = false;
					if(typeClass == PactInteger.class) {
						int csv = c.getField(1, PactInteger.class).getValue();
						int tsv = t.getField(1, PactInteger.class).getValue();
						if(csv != tsv) {
							flag.setValue(GroupResult.BOTH_UNEQUAL.ordinal());
							ret.setField(0, c.getField(0, PactString.class));
							ret.setField(1, new PactInteger(csv));
							ret.setField(2, new PactInteger(tsv));
							ret.setField(3, flag);
							match = true;
							out.collect(ret);
						}
					} else if(typeClass == PactDouble.class) {
						double csv = c.getField(1, PactDouble.class).getValue();
						double tsv = t.getField(1, PactDouble.class).getValue();
						if(Math.abs(csv-tsv) > 1 ) { // less then 1 distance. 
							flag.setValue(GroupResult.BOTH_UNEQUAL.ordinal());
							ret.setField(0, c.getField(0, PactString.class));
							ret.setField(1, new PactDouble(csv));
							ret.setField(2, new PactDouble(tsv));
							ret.setField(3, flag);
							match = true;
							out.collect(ret);
						}
					} else if(typeClass == PactString.class) {
						String a = c.getField(1, PactString.class).getValue();
						String b = t.getField(1, PactString.class).getValue();
						if(!a.equals(b)) {
							flag.setValue(GroupResult.BOTH_UNEQUAL.ordinal());
							ret.setField(0, c.getField(0, PactString.class));
							ret.setField(1, new PactString(a));
							ret.setField(2, new PactString(b));
							ret.setField(3, flag);
							match = true;
							out.collect(ret);
						}
					} else {
						throw new RuntimeException("Unknown type");
					}
					if(match) {
						cIt.remove();
						tIt.remove();
					}
				}
			}
			cIt = csvs.iterator();
			while(cIt.hasNext()) {
				PactRecord c = cIt.next();
				flag.setValue(GroupResult.ONLY_CSV.ordinal());
				c.setField(3, flag);
				out.collect(c);
			}
			tIt = csvs.iterator();
			while(tIt.hasNext()) {
				PactRecord t = tIt.next();
				flag.setValue(GroupResult.ONLY_TSV.ordinal());
				t.setField(3, flag);
				out.collect(t);
			}
		}
		
	}
	
	public static class FlagFilter extends MapStub {

		int flag;
		
		public FlagFilter(int flag) {
			super();
			this.flag = flag;
		}

		@Override
		public void map(PactRecord record, Collector<PactRecord> out)
				throws Exception {
			int rFlag = record.getField(3, PactInteger.class).getValue();
			if(rFlag == this.flag) {
				out.collect(record);
			}
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
			typeClass = PactInteger.class;
		}
		if(type.equals("double")) {
			typeClass = PactDouble.class;
		}
		if(type.equals("string")) {
			typeClass = PactString.class;
		}
		if(typeClass == null) throw new IllegalArgumentException("Unknown type");
		
		FileDataSource csv = new FileDataSource(RecordInputFormat.class, inputCSV, "CSV");
		RecordInputFormat.configureRecordFormat(csv)
		.recordDelimiter('\n')
		.fieldDelimiter(',')
		.field(PactString.class, 0)
		.field(typeClass, csvFieldID);
		
		FileDataSource tsv = new FileDataSource(RecordInputFormat.class, inputTSV, "TSV");
		RecordInputFormat.configureRecordFormat(tsv)
		.recordDelimiter('\n')
		.fieldDelimiter('\t')
		.field(PactString.class, 0)
		.field(typeClass, tsvFieldID);
		
		ReduceContract countCsv = ReduceContract.builder(new Count("CSV Input Count")).input(csv).build();
		ReduceContract countTsv = ReduceContract.builder(new Count("TSV Input Count")).input(tsv).build();
		
		
		
		CoGroupContract cross = CoGroupContract.builder(new CoGroup(typeClass), typeClass, 1, 1)
				.input1(csv).input2(tsv).build();
		MapContract unequal = MapContract.builder(new FlagFilter(GroupResult.BOTH_UNEQUAL.ordinal()))
				.input(cross).build();
		
		MapContract onlyCSV = MapContract.builder(new FlagFilter(GroupResult.ONLY_CSV.ordinal()))
				.input(cross).build();
		
		MapContract onlyTSV = MapContract.builder(new FlagFilter(GroupResult.ONLY_TSV.ordinal()))
				.input(cross).build();
		
		ReduceContract countWrong = ReduceContract.builder(new Count("Total wrong count")).input(unequal).build();
		ReduceContract csvWrongCount = ReduceContract.builder(new Count("CSV only count")).input(onlyCSV).build();
		ReduceContract tsvWrongCount = ReduceContract.builder(new Count("TSV only count")).input(onlyTSV).build();
		
		FileDataSink actualResult = new FileDataSink(RecordOutputFormat.class, output, verify, "Write result");
		ConfigBuilder b = RecordOutputFormat.configureRecordFormat(actualResult).recordDelimiter('\n')
				.fieldDelimiter(',').lenient(true).field(PactString.class, 0);
			b.field(typeClass, 1);
			b.field(typeClass, 2);
		
		List<GenericDataSink> out = new ArrayList<GenericDataSink>(2);
		
		MapContract union = MapContract.builder(Identity.class).input(countCsv,countTsv,countWrong,csvWrongCount,tsvWrongCount).name("merge").build();
		FileDataSink statsOut = new FileDataSink(RecordOutputFormat.class, output+"-stats", "Write stats");
		statsOut.setDegreeOfParallelism(1);
		statsOut.addInput(union);
		RecordOutputFormat.configureRecordFormat(statsOut).recordDelimiter('\n')
				.fieldDelimiter('\t').lenient(true).field(PactString.class, 0).field(PactInteger.class, 1);
		
		out.add(statsOut);
		out.add(actualResult);
		
		
			
			
		Plan plan = new Plan(out, "Generic Comparator");
		plan.setDefaultParallelism(noSubTasks);
		return plan;
	}


	
	
	public static void main(String[] args) throws Exception {
		FlexCmpCross ic = new FlexCmpCross();
		// final String PATH = "file:///home/robert/Projekte/ozone/result-verification/";
		final String PATH = "file:///home/robert/Projekte/ozone/work/result-verification/";
		Plan toExecute = ic.getPlan("1","double",
				PATH + "csv", "1",
				PATH + "tsv", "1",
				PATH + "result");
		LocalExecutor l = new LocalExecutor();
		System.err.println("Starting");
		l.start();
		l.executePlan(toExecute);
		
		Plan string = ic.getPlan("1","string",
				PATH + "string.csv", "1",
				PATH + "string.tsv", "1",
				PATH + "string.result");
		
		l.executePlan(string);
		System.exit(0);
	}
}
