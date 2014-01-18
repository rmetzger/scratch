package de.robertmetzger;

import org.apache.commons.lang.RandomStringUtils;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.CollectionDataSource;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.util.SerializableIterator;
import eu.stratosphere.api.io.jdbc.JDBCOutputFormat;
import eu.stratosphere.api.java.record.io.DelimitedOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.types.FloatValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;


/**
 * Prepare your database:
 * 
 * > mysql
 * > CREATE DATABASE "database";
 * > CREATE TABLE example (id INT,data VARCHAR(100) );
 * 
 * @author robert
 *
 */

public class Job implements Program {

	public static class DataGen extends SerializableIterator<Object> {
		private static final long serialVersionUID = 1L;
		private int cnt = 0;
		@Override
		public boolean hasNext() {
			return cnt++ < 1000000;
		}
		@Override
		public Object next() {
			Object[] r = {cnt, RandomStringUtils.randomAlphabetic(15)};
			return r;
		}
	}
	
    public Plan getPlan(String... args) {
    	CollectionDataSource src = new CollectionDataSource(new DataGen());
        GenericDataSink sink = new GenericDataSink(new JDBCOutputFormat(), "Data Output");
        sink.setInput(src);
        JDBCOutputFormat.configureOutputFormat(sink)
                .setDriver("com.mysql.jdbc.Driver")
                .setUrl("jdbc:mysql://localhost/database?user=root&useServerPrepStmts=false&rewriteBatchedStatements=true")
                .setQuery("insert into example (id,data) values (?,?)")
                .setClass(IntValue.class)
                .setClass(StringValue.class);
		return new Plan(sink, "Generate Data to a database");
    }

    // You can run this using:
    // mvn exec:exec -Dexec.executable="java" -Dexec.args="-cp %classpath de.robertmetzger.RunJob <args>"
    public static void main(String[] args) throws Exception {
        Job tut = new Job();
        Plan toExecute = tut.getPlan(args);

        JobExecutionResult result = LocalExecutor.execute(toExecute);
        System.out.println("runtime:  " + result.getNetRuntime());
    }
}