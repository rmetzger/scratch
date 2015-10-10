package com.dataartisans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

import java.io.IOException;

/**
 * Created by robert on 9/8/15.
 */
public class ArnaudHCat {

	/*public static void main(String[] args) throws IOException {
		final org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance();
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final HadoopInputFormat<NullWritable, DefaultHCatRecord> inputFormat = new HadoopInputFormat<NullWritable,
						DefaultHCatRecord>(
				(InputFormat) HCatInputFormat.setInput(job, "dbName", "tableName"), //
				NullWritable.class, //
				DefaultHCatRecord.class, //
				job);

		DataSet<String> cluster = null;
		final HCatSchema inputSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
		@SuppressWarnings("serial")
		final DataSet<String> dataSet = cluster
				.createInput(inputFormat)
				.flatMap(new FlatMapFunction<Tuple2<NullWritable, DefaultHCatRecord>, T>() {
					@Override
					public void flatMap(Tuple2<NullWritable, DefaultHCatRecord> value, Collector<T> out) throws Exception { // NOPMD
						final T record = createBean(value.f1, inputSchema);
						out.collect(record);
					}
				}).returns(beanClass);
	} */
}
