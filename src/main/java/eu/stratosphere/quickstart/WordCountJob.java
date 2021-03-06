package eu.stratosphere.quickstart;

import java.util.Iterator;

import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * A sample word count Stratosphere job.
 * <p>
 * You can run this sample from your IDE using the main() method. The LocalExecutor starts a Stratosphere instance out
 * of your IDE.
 * <p>
 * The two inner classes SplitWords and CountWords provide sample user logic to count the words. Please check the
 * respective comments for details.
 */
public class WordCountJob implements PlanAssembler, PlanAssemblerDescription {

	// -- SAMPLE OPERATORS with word counting logic ---------------------------

	
public class FilterOrders extends MapStub {
	@Override
	public void map(PactRecord order, Collector<PactRecord> out)
			throws Exception {
		PactString date = order.getField(Orders.DATE_IDX, PactString.class);
		if(date.getValue().equals("11.20.2013")) {
			out.collect(order);
		}
	}
}

	public static class SplitWords extends MapStub {

		// resusable mutable objects
		private final PactRecord output = new PactRecord();

		private final PactString word = new PactString();

		private final PactInteger one = new PactInteger(1);

		/**
		 * Splits every line by whitespace and emits a (word, 1) record for
		 * each word.
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> collector) {
			// read the first field of the record
			// note: PactRecord field indexes start with 0
			PactString line = record.getField(0, PactString.class);

			// split every line by whitespace
			for (String currentWord : line.getValue().split(" ")) {
				// output: (word, 1) record
				this.word.setValue(currentWord);

				this.output.setField(0, this.word);
				this.output.setField(1, this.one);

				collector.collect(this.output);
			}
		}

	}

	public static class CountWords extends ReduceStub {

		private final PactInteger count = new PactInteger();

		/**
		 * Counts the ones for each word and emits a (word, sum) record for
		 * each word.
		 */
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) throws Exception {
			PactRecord current = null;

			int sum = 0;
			while (records.hasNext()) {
				current = records.next();
				sum += current.getField(1, PactInteger.class).getValue();
			}

			// output: (word, sum) record
			// note: the current record has the word already as first field (index 0).
			// therefore we only set the second field (index 1) to the sum.
			this.count.setValue(sum);
			current.setField(1, this.count);

			out.collect(current);
		}

	}

	// -- SAMPLE PLAN which connects the operators ----------------------------

	/**
	 * Connects the operators and returns the resulting plan.
	 * 
	 * <pre>
	 * +------------+    +-------------+    +----------------+    +----------+
	 * |   Source   | => | MapContract | => | ReduceContract | => |   Sink   |
	 * +------------+    +-------------+    +----------------+    +----------+
	 *  foo bar foo       (foo, 1)           (foo, 2)              foo, 2
	 *                    (bar, 1)           (bar, 1)              bar, 1
	 *                    (foo, 1)
	 * </pre>
	 */
	public Plan getPlan(String... args) {
		String inputPath = (args.length >= 1 ? args[0] : "");
		String outputPath = (args.length >= 2 ? args[1] : "");
		String ordersPath, customersPath;
		// input: treat input as text with TextInputFormat
		
		
FileDataSource customers = new FileDataSource(TextInputFormat.class, customersPath);
FileDataSource orders = new FileDataSource(TextInputFormat.class, ordersPath);

MapContract ordersFiltered = MapContract.builder(FilterOrders.class).input(orders).build();
ReduceContract groupedCustomers = ReduceContract.builder(GroupCustomers.class)
	.input(customers)
	.keyField(PactInteger.class, 0)
	.build();
MatchContract joined = MatchContract.builder(JoinOnCustomerid.class,PactInteger.class, 0,0)
	.input1(ordersFiltered).input2(groupedCustomers).build();
ReduceContract orderBy = ReduceContract.builder(MaxSum.class)
	.input(joined)
	.keyField(PactInteger.class, 0)
	.build();
FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, orderBy, "output: word counts");
		
		
		// configure the record output format (at least one field needs to be specified) 
		RecordOutputFormat.configureRecordFormat(sink)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			// the following lines configure the fields to write (in given order)
			// e.g. word (field 0), count (field 1)
			.field(PactString.class, 0)        // <--+ swap lines 
			.field(PactInteger.class, 1);      // <--+    if you want (count, word) instead

		Plan plan = new Plan(sink, "WordCount Sample Job");
		plan.setDefaultParallelism(1);

		return plan;
	}

	public String getDescription() {
		return "Usage: [input] [output]";
	}

	// -- RUNNING IN LOCAL MODE -----------------------------------------------

	public static void execute(Plan toExecute) throws Exception {
		LocalExecutor executor = new LocalExecutor();
		executor.start();
		long runtime = executor.executePlan(toExecute);
		System.out.println("runtime:  " + runtime);
		executor.stop();
	}

	public static void main(String[] args) throws Exception {
		WordCountJob tut = new WordCountJob();
		String inputPath = "file:///path/to/input";
		String outputPath = "file:///path/to/output";

		Plan toExecute = tut.getPlan(inputPath, outputPath);
		execute(toExecute);
		System.exit(0);
	}
}