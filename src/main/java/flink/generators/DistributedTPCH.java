package flink.generators;

import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.Order;
import io.airlift.tpch.OrderGenerator;
import io.airlift.tpch.Part;
import io.airlift.tpch.PartGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.SplittableIterator;

public class DistributedTPCH {
	private double scale;
	private ExecutionEnvironment env;


	public DistributedTPCH(ExecutionEnvironment env) {
		this.env = env;
	}

	public void setScale(double scale) {
		this.scale = scale;
	}

	public double getScale() {
		return scale;
	}

	public DataSet<Part> generateParts() {
		return getGenerator(PartGenerator.class, Part.class);
	}

	public DataSet<LineItem> generateLineItems() {
		return getGenerator(LineItemGenerator.class, LineItem.class);
	}
	public DataSet<Order> generateOrders() {
		return getGenerator(OrderGenerator.class, Order.class);
	}

	public <T> DataSet<T> getGenerator(Class<? extends Iterable<T>> generatorClass, Class<T> type) {
		SplittableIterator<T> si = new TPCHGeneratorSplittableIterator(scale, env.getDegreeOfParallelism(), generatorClass);
		return env.fromParallelCollection(si, type).name("Generator: "+generatorClass);
	}
}
