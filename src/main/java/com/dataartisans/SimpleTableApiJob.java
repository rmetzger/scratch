package com.dataartisans;

import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableConfig;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleTableApiJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple3<Integer, String, Double>> input = env.fromElements(
                Tuple3.of(1, "a", 1.0),
                Tuple3.of(2, "b", 2.0),
                Tuple3.of(3, "c", 3.0));


        TableConfig tblConfig = new TableConfig();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env, tblConfig);

        Table inputTable = tableEnv.fromDataStream(input, "a, b, c");

        tableEnv.registerDataStream("foobar", input, "a, b, c");

        Table timesThree = inputTable.select("a * 3");
        tableEnv.toDataStream(timesThree, Row.class).print();

        // not working because of shading problem
//        Table resultTable = tableEnv.sql("SELECT STREAM * FROM foobar");

        Table resultTable = inputTable.filter("a * .001E3 * c >= -15e-1 * (-1 + 2) * -1").select("b as c");
        Table resultTableSql = tableEnv.sql("SELECT STREAM b as c FROM foobar WHERE a * .001E3 * c >= -15e-1 * (-1 + 2) * -1");

        //    tableEnv.toDataStream(resultTable, Row.class).print();
        tableEnv.toDataStream(resultTableSql, Row.class).print();

        env.execute();
    }
}