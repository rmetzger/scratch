package example.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.ArrayList;
import java.util.List;

public class JaccardSimilarityMeasureData {

	public static final String EDGES = "1	2\n" + "1	3\n" + "1	4\n" + "1	5\n" + "2	3\n" + "2	4\n" +
			"2	5\n" + "3	4\n" + "3	5\n" + "4	5";

	public static DataSet<Edge<Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Edge<Long, Double>> edges = new ArrayList<>();
		edges.add(new Edge<Long, Double>(1L, 2L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 3L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(1L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 3L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(2L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(3L, 4L, new Double(0)));
		edges.add(new Edge<Long, Double>(3L, 5L, new Double(0)));
		edges.add(new Edge<Long, Double>(4L, 5L, new Double(0)));

		return env.fromCollection(edges);
	}

	private JaccardSimilarityMeasureData() {}
}
