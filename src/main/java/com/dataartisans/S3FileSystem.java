package com.dataartisans;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class S3FileSystem {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment ee = ExecutionEnvironment.createLocalEnvironment();
		DataSet<String> myLines = ee.readTextFile("s3n://rm-travis-logs/NiFi_Flink.xml");
		myLines.print();
	}
}
