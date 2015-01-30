/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package flink.generators.programs;

import flink.generators.core.DistributedTPCH;
import flink.generators.core.TpchEntityFormatter;
import io.airlift.tpch.Customer;
import io.airlift.tpch.CustomerGenerator;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.Nation;
import io.airlift.tpch.NationGenerator;
import io.airlift.tpch.Order;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartGenerator;
import io.airlift.tpch.Region;
import io.airlift.tpch.RegionGenerator;
import io.airlift.tpch.Supplier;
import io.airlift.tpch.SupplierGenerator;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

public class TPCHGenerator {
	public static void main(String[] args) throws Exception {
		// Parse and handle arguments
		ArgumentParser ap = ArgumentParsers.newArgumentParser("Distributed TPCH");
		ap.defaultHelp(true);
		ap.addArgument("-s", "--scale").setDefault(1.0).help("TPC H Scale (final Size in GB)");
		ap.addArgument("-p","--parallelism").setDefault(1).help("Parallelism for program");
		ap.addArgument("-e", "--extension").setDefault(".csv").help("File extension for generated files");
		ap.addArgument("-o", "--outpath").setDefault("/tmp/").help("Output directory");
		
		Namespace ns = null;
        try {
            ns = ap.parseArgs(args);
        } catch (ArgumentParserException e) {
            ap.handleError(e);
            System.exit(1);
        }
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(ns.getInt("parallelism"));
		DistributedTPCH gen = new DistributedTPCH(env);
		gen.setScale(ns.getDouble("scale"));

		String base = ns.getString("outpath");
		String ext = ns.getString("extension");
		gen.generateParts().writeAsFormattedText(base + "parts" + ext, new TpchEntityFormatter());
		gen.generateLineItems().writeAsFormattedText(base + "lineitems" + ext, new TpchEntityFormatter());
		gen.generateOrders().writeAsFormattedText(base + "orders" + ext, new TpchEntityFormatter());
		gen.generateSuppliers().writeAsFormattedText(base + "suppliers" + ext, new TpchEntityFormatter());
		gen.generatePartSuppliers().writeAsFormattedText(base + "partsuppliers" + ext, new TpchEntityFormatter());
		gen.generateRegions().writeAsFormattedText(base + "regions" + ext, new TpchEntityFormatter());
		gen.generateNations().writeAsFormattedText(base + "nations" + ext, new TpchEntityFormatter());
		gen.generateCustomers().writeAsFormattedText(base + "customers" + ext, new TpchEntityFormatter());

		env.execute("Distributed TPCH Generator, Scale = "+gen.getScale());
	}





}
