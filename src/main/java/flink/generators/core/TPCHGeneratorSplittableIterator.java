package flink.generators.core;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Resources;
import io.airlift.tpch.Distribution;
import io.airlift.tpch.Distributions;
import io.airlift.tpch.TextPool;
import org.apache.flink.util.SplittableIterator;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.tpch.DistributionLoader.loadDistribution;

public class TPCHGeneratorSplittableIterator<T> implements SplittableIterator<T> {
	private double scale;
	private int degreeOfParallelism;
	private Class<? extends Iterable<T>> generatorClass;

	public TPCHGeneratorSplittableIterator(double scale, int degreeOfParallelism, Class<? extends Iterable<T>> generatorClass) {
		Preconditions.checkArgument(scale > 0, "Scale must be > 0");
		Preconditions.checkArgument(degreeOfParallelism > 0, "Parallelism must be > 0");

		this.scale = scale;
		this.degreeOfParallelism = degreeOfParallelism;
		this.generatorClass = generatorClass;
	}

	@Override
	public Iterator<T>[] split(int numPartitions) {
		if(numPartitions > this.degreeOfParallelism) {
			throw new IllegalArgumentException("Too many partitions requested");
		}
		Iterator<T>[] iters = new Iterator[numPartitions];
		for(int i = 1; i <= numPartitions; i++) {
			iters[i - 1] = new TPCHGeneratorSplittableIterator(i, numPartitions, scale, generatorClass);
		}
		return iters;
	}

	@Override
	public Iterator<T> getSplit(int num, int numPartitions) {
		if (numPartitions < 1 || num < 0 || num >= numPartitions) {
			throw new IllegalArgumentException();
		}
		return split(numPartitions)[num];
	}

	@Override
	public int getMaximumNumberOfSplits() {
		return this.degreeOfParallelism;
	}

	//------------------------ Iterator -----------------------------------
	private Iterator<T> iter;

	public TPCHGeneratorSplittableIterator(int partNo, int totalParts, double scale, Class<? extends Iterable<T>> generatorClass) {
		try {

			URL resource = Resources.getResource(Distribution.class, "dists.dss");
			checkState(resource != null, "Distribution file 'dists.dss' not found");
			Distributions distributions = new Distributions(loadDistribution(Resources.asCharSource(resource, Charsets.UTF_8)));
			// use per-thread text pool
			TextPool smallTextPool = new TextPool(1 * 1024 * 1024, distributions); // 1 MB txt pool
			Constructor<? extends Iterable<T>> generatorCtor;

			generatorCtor = generatorClass.getConstructor(double.class, int.class, int.class, Distributions.class, TextPool.class);
			Iterable<T> generator = generatorCtor.newInstance(scale, partNo, totalParts, distributions, smallTextPool);
			iter = generator.iterator();
		} catch (Throwable e) {
			throw new RuntimeException("Unable to create generator "+generatorClass, e);
		}
	}
	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public T next() {
		return iter.next();
	}
}
