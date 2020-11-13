package de.robertmetzger;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.XORShiftRandom;

import com.google.common.collect.Lists;

import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

//////////////////////////////////////////////////////////////////////////////////
/////////////////////// NOTE: Don't share this job publicly //////////////////////
//////////////////////////////////////////////////////////////////////////////////

/**
 * Main maintainer of this job: Robert
 *
 * This job has been implemented initially for testing the 1.1.4 (1.2.0) release.
 * The job is designed to be misbehaved. It puts pressure on various parts on the system.
 *
 * Reference job: https://files.slack.com/files-pri/T0763FK1B-F31SW6R6E/screen_shot_2016-11-12_at_10.08.00_pm.png
 *
 * These are the problems the job has:
 * A) Selective backpressure on some channels (randomly) (Done)
 * B) Large closure as part of at least one UDF (Done)
 * C) Large topology (for deploying may tasks) / disabled chaining
 * D) Randomly blocking cancel, ignoring interrupts
 * E) Big value in state (for RocksDB) (~20 MB)
 * F) coMap with broadcast / (shuffle / keyed) input (with remote / local read channels) (Done)
 * G) Big records through the network stack (Done)
 * H) Big state (bigger than memory)
 * I) Types that need a kryo and kryo with a custom serializer.
 *
 *
 * The application is a dynamic filter (coMap) that filters for events from product sales
 * and impressions on those product.
 * There are way more impression events than sale events.
 *
 * Running on a cluster:
 * ./flink-1.1.3/bin/flink run -c com.dataartisans.heavymisbehaved.HeavyMisbehavedJob -m yarn-cluster -yn 16 -ytm 2048 ./flink-testing-jobs/basic-jobs/target/flink-basic-testing-jobs-0.1-SNAPSHOT.jar ./flink-testing-jobs/heavymisbehaved.properties
 *
 */
public class HeavyMisbehavedJob {

    private static final Logger LOG = LoggerFactory.getLogger(HeavyMisbehavedJob.class);

    public static void main(String[] args) throws Exception {

        // for AppManager, we need to parse the config options differently.
        final ParameterTool pt;
        if(args.length > 1) {
            LOG.info("Parsing parameters from args");
            pt = ParameterTool.fromArgs(args);
        } else {
            LOG.info("Parsing parameters from properties");
            pt = ParameterTool.fromPropertiesFile(args[0]);
        }


        StreamExecutionEnvironment see;
        if(pt.getBoolean("local", true)) {
            Configuration config = new Configuration();
            config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
            // previously 15000 * 32K - so let's fix it to this amount for now
        //    config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, 491_520_000L);
        //    config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 491_520_000L);
            see = StreamExecutionEnvironment.createLocalEnvironment(8, config);
            see.setParallelism(pt.getInt("par", 1));
        } else {
            see = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        see.getConfig().setGlobalJobParameters(pt);
        see.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(pt.getInt("restartAttempts", 50),
                        pt.getLong("restartDelay", 5_000L)));
        // register custom kryo serializer for a type that Kryo can not serialize by default.
        Collection<String> unmodifiableColl = Collections.unmodifiableCollection(Lists.newArrayList("Test"));
        see.getConfig().addDefaultKryoSerializer(unmodifiableColl.getClass(), UnmodifiableCollectionsSerializer.class);
        if(pt.getBoolean("disableOperatorChaining", true)) {
            see.disableOperatorChaining();
        }
        see.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;
        if(pt.getBoolean("atLeastOnce", false)) {
            checkpointMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        see.enableCheckpointing(pt.getLong("checkpointInterval", 30_000L), checkpointMode);
        see.getCheckpointConfig().setMinPauseBetweenCheckpoints(pt.getLong("minPauseBetweenCheckpoints", 30_000L));
        see.getCheckpointConfig().setCheckpointTimeout(pt.getLong("checkpointTimeout", 60_000L * 10)); // 10 minutes

        if(pt.has("rocksdb")) {
            boolean incremental = pt.has("rocksdb.incremental");
            if(incremental) {
                System.out.println("[rocksdb] Enabling incremental");
            }

            RocksDBStateBackend rocks = new RocksDBStateBackend(pt.getRequired("rocksdb"), incremental);
            if(pt.has("rocksdb.options")) {
                rocks.setPredefinedOptions(PredefinedOptions.valueOf(pt.get("rocksdb.options")));
            }
            see.setStateBackend(rocks);
        }

        if(pt.has("async-fs")) {
            System.out.println("[FsStateBackend] Enabling AsyncFS");
            see.setStateBackend(new FsStateBackend(pt.get("async-fs"), true));
        }

        boolean enableComplexMode = pt.getBoolean("enableComplexMode", false);

        // slow source
        DataStream<ControlEvent> controlEvents = see.addSource(new ControlEventsGenerator(pt)).name("control events generator");
        controlEvents.addSink(new MisbehavedSink<ControlEvent>(pt)).name("control events sink"); // write the control events to a sink

        // slow source (containing updates from the warehouse about the product status)
        DataStream<WarehouseUpdates> warehouseStockStatus = null;
        if(enableComplexMode) {
            warehouseStockStatus = see.addSource(new WarehouseUpdatesGenerator(pt)).name("warehouse updates generator");
        }

        // slow source
        DataStream<SaleEvent> sales = see.addSource(new SaleEventGenerator(pt)).name("sale event generator");
        byte[] salesFilterClosureBytes = new byte[pt.getInt("closureSize", 1048576)]; // 1 MB by default.
        DataStream<SaleEvent> filteredSales = sales.filter(new SalesFilter(salesFilterClosureBytes)).name("sale event filter");
        filteredSales.addSink(new MisbehavedSink<SaleEvent>(pt)).name("filtered sales sink"); // this is super important for our business :)

        // very fast source
        DataStream<ProductImpressions> impressions = see.addSource(new ProductImpressionsGenerator(pt)).name("product impressions generator (fast)");


        // create a filter just for sales events:
        DataStream<Tuple2<ControlEvent, SaleEvent>> salesFilter = controlEvents.broadcast().connect(sales).flatMap(new DynamicSalesFilter(pt));
        salesFilter.addSink(new MisbehavedSink<Tuple2<ControlEvent, SaleEvent>>(pt));

        // create a special operator consuming keyed controlEvents and other events
        if(enableComplexMode) {
            DataStream<Object> invalidWarehouseUpdates = controlEvents.keyBy(new KeySelector<ControlEvent, Integer>() {
                private static final long serialVersionUID = 2126911547691694903L;

                @Override
                public Integer getKey(ControlEvent value) throws Exception {
                    return value.type.ordinal();
                }
            }).connect(warehouseStockStatus.broadcast()).flatMap(new FilterInvalidItemsFromWarehouse());
            invalidWarehouseUpdates.addSink(new MisbehavedSink<>(pt));
        }

        // connect control, sales and impressions stream to a coFlatMap for the filtering
        DataStream<Tuple2<ControlEvent, SaleEvent>> unionableControl = controlEvents.map(new ConvertControlEvent()).name("ctrl event mapping");
        DataStream<Tuple2<ControlEvent, SaleEvent>> unionableSales = sales.map(new ConvertSaleEvent()).name("sale event mapping");

        DataStream<Tuple2<ControlEvent, SaleEvent>> controlAndSales = unionableControl.union(unionableSales).union(salesFilter).broadcast();

        ConnectedStreams<Tuple2<ControlEvent, SaleEvent>, ProductImpressions> filterStream = controlAndSales.connect(impressions.rebalance());

        // the actual filter
        DataStream<FilterMatch> matches = filterStream.flatMap(new DynamicFilter(pt)).name("dynamic filter");
        matches.addSink(new MisbehavedSink<FilterMatch>(pt));

        matches.keyBy("id").flatMap(new RichFlatMapFunction<FilterMatch, ControlEvent>() {
            private static final long serialVersionUID = -7362865462646921004L;
            byte[] refState;
            ValueState<byte[]> keyedState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                refState = new byte[pt.getInt("keySize", 48)];
                keyedState = getRuntimeContext().getState(new ValueStateDescriptor<byte[]>("byte", byte[].class, new byte[]{0}));
            }

            @Override
            public void flatMap(FilterMatch value, Collector<ControlEvent> out) throws Exception {
                keyedState.update(refState);
            }
        });

        see.execute("Misbehaved Job");
    }

    private abstract static class Filter implements Serializable {
        private static final long serialVersionUID = -1007220322397073991L;
        public int id;

        public abstract boolean match(ProductDescriptor pd);

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Filter filter = (Filter) o;

            return id == filter.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    private static class VoidFilter extends Filter {
        private static final long serialVersionUID = 2269145376753663695L;
        private final static Collection<String> unmodifiableColl = Collections.unmodifiableCollection(Lists.newArrayList("Test"));

        private byte[] workload;
        // TODO This is unsupported in Flink 1.4.0
      //  private Collection<String> kryoNonSerializableField;

        public VoidFilter(byte[] bytes) {
            workload = bytes;
      //      kryoNonSerializableField = unmodifiableColl;
        }

        @Override
        public boolean match(ProductDescriptor pd) {
            return false;
        }
    }

    private static class ProductFilter extends Filter {
        private static final long serialVersionUID = 7509499035200109247L;
        private final long pIdFrom;
        private final long pIdTo;

        // from: inclusive, to: exclusive
        public ProductFilter(long pIdFrom, long pIdTo) {
            this.pIdFrom = pIdFrom;
            this.pIdTo = pIdTo;
        }
        public boolean match(ProductDescriptor se) {
            return se.productId >= pIdFrom && se.productId < pIdTo;
        }
    }

    private static class ControlEvent {
        enum Type {
            ADD_FILTER, REMOVE_FILTER, AUDIT_EVENT
        }
        Type type;
        public Filter filter;
    }

    private static class ProductDescriptor {
        protected long productId;
    }

    private static class SaleEvent extends ProductDescriptor {
        private byte[] workload;
    }

    private static class ProductImpressions extends ProductDescriptor {
    }

    public static class FilterMatch {
        public long id;
        public FilterMatch() {

        }
    }

    private static class ControlEventsGenerator extends RichParallelSourceFunction<ControlEvent>
    implements ListCheckpointed<Filter> {
        private static final long serialVersionUID = -9189601301679592269L;
        private final Random RND = new XORShiftRandom(UUID.randomUUID().getLeastSignificantBits());
        private int targetNumFilters;
        private int voidFilterSize;
        private final ParameterTool pt;
        private boolean running = true;
        private long EPS;
        private float targetMatchRatio;
        private long targetNumProducts;
        private int largeWorkloadSize;

        // runtime variables
        private transient List<Filter> filters;



        public ControlEventsGenerator(ParameterTool pt) {
            this.pt = pt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            targetNumFilters = pt.getInt("numFilters", 10_000);
            targetMatchRatio = pt.getFloat("matchRatio", 0.01f);
            targetNumProducts = pt.getLong("numProducts", 100_000_000L);
            voidFilterSize = pt.getInt("controlEventsNoiseEventSize", 1024);
            largeWorkloadSize = pt.getInt("controlEventsNoiseLargeEventSize", 1024 * 1024 * 25);
            EPS = pt.getLong("controlEventEPS", 200) / getRuntimeContext().getNumberOfParallelSubtasks();

            if(filters == null) {
                LOG.info("[Source] Creating empty filter list");
                filters = new ArrayList<>(targetNumFilters);
            }
        }

        @Override
        public void run(SourceContext<ControlEvent> ctx) throws Exception {
            if(getRuntimeContext().getIndexOfThisSubtask() == 0) {
                ElementsPerSecond perSecond = new ElementsPerSecond(EPS);
                while (running) {
                    ControlEvent ctr = new ControlEvent();
                    if(filters.size() < targetNumFilters) {
                        // add filter
                        ctr.type = ControlEvent.Type.ADD_FILTER;
                        ctr.filter = new ProductFilter(0, (long)(targetNumProducts * targetMatchRatio));
                        synchronized (ctx.getCheckpointLock()) {
                            filters.add(ctr.filter);
                            ctx.collect(ctr);
                        }
                    } else {
                        // remove a filter
                        synchronized (ctx.getCheckpointLock()) {
                            Filter removed = filters.get(RND.nextInt(filters.size()));
                            ctr.type = ControlEvent.Type.REMOVE_FILTER;
                            ctr.filter = removed;
                            ctx.collect(ctr);
                        }
                    }
                    perSecond.reportElement();
                }
            } else {
                // we only send some bullshit events here
                ElementsPerSecond perSecond = new ElementsPerSecond(EPS);
                byte[] voidWL = new byte[voidFilterSize];
                byte[] largeWL = new byte[largeWorkloadSize];
                while (running) {
                    ControlEvent ctr = new ControlEvent();
                    ctr.type = ControlEvent.Type.AUDIT_EVENT;
                    if(RND.nextInt(500) == 0) {
                        ctr.filter = new VoidFilter(largeWL);
                    } else {
                        ctr.filter = new VoidFilter(voidWL);
                    }
                    ctx.collect(ctr);
                    perSecond.reportElement(); // this one sleeps accordingly
                }
            }
        }

        @Override
        public void cancel() {
            // lets first burn some CPU time here to block and ignore interrupts
            int sec = new Random().nextInt(120);
            LOG.info("This generator is blocking hard for the next {} seconds", sec);
            Deadline dl = new FiniteDuration(sec, TimeUnit.SECONDS).fromNow();
            while(dl.hasTimeLeft()) {
                Math.atan(Math.sqrt(Math.pow(Math.pow(2.8192, 10),2)));
            }
            running = false;
        }

        @Override
        public List<Filter> snapshotState(long l, long l1) throws Exception {
            return filters;
        }

        @Override
        public void restoreState(List<Filter> list) throws Exception {
            filters = list;
        }
    }

    private static class MisbehavedSink<T> implements SinkFunction<T> {
        private static final long serialVersionUID = 1069708497995327878L;

        public MisbehavedSink(ParameterTool pt) {

        }

        @Override
        public void invoke(T value) throws Exception {
            // for now, its not misbehaved :)
        }
    }



    private static class SaleEventGenerator extends RichParallelSourceFunction<SaleEvent> {
        private static final long serialVersionUID = -587217683587475765L;
        private final ParameterTool pt;
        private long EPS;
        private boolean running = true;
        private int workloadSize;
        private final Random RND = new XORShiftRandom(UUID.randomUUID().getLeastSignificantBits());
        private long targetNumProducts;

        public SaleEventGenerator(ParameterTool pt) {
            this.pt = pt;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            EPS = pt.getLong("saleEventEPS", 4000) / getRuntimeContext().getNumberOfParallelSubtasks();
            targetNumProducts = pt.getLong("numProducts", 100_000_000L);
            workloadSize = pt.getInt("saleEventSize", 48);
        }

        @Override
        public void run(SourceContext<SaleEvent> ctx) throws Exception {
            ElementsPerSecond perSecond = new ElementsPerSecond(EPS);
            byte[] wl = new byte[workloadSize];
            while(running) {
                SaleEvent se = new SaleEvent();
                se.workload = wl;
                se.productId = (long)(RND.nextDouble()*targetNumProducts);
                ctx.collect(se);
                perSecond.reportElement();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }



    private static class ProductImpressionsGenerator extends RichParallelSourceFunction<ProductImpressions> {
        private static final long serialVersionUID = -518831355762013558L;
        private final long targetNumProducts;
        private final Random RND = new XORShiftRandom(UUID.randomUUID().getLeastSignificantBits());
        private boolean running = true;

        public ProductImpressionsGenerator(ParameterTool pt) {
            targetNumProducts = pt.getLong("numProducts", 100_000_000L);
        }

        // this runs as fast as possible, with very small records
        @Override
        public void run(SourceContext<ProductImpressions> ctx) throws Exception {
            while(running) {
                ProductImpressions pi = new ProductImpressions();
                pi.productId = (long)(RND.nextDouble()*targetNumProducts);
                ctx.collect(pi);
                Thread.yield();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    private static class SalesFilter implements FilterFunction<SaleEvent> {
        private static final long serialVersionUID = 5780570669644492819L;
        private final byte[] salesFilterClosureBytes;

        public SalesFilter(byte[] salesFilterClosureBytes) {
            this.salesFilterClosureBytes = salesFilterClosureBytes;
        }

        @Override
        public boolean filter(SaleEvent value) throws Exception {
            if(salesFilterClosureBytes == null) {
                throw new RuntimeException("Expected bytes to be present at runtime");
            }
            return true; // to be very honest here, the filter doesn't do anything for the app logic.
        }
    }

    private static class ConvertSaleEvent implements MapFunction<SaleEvent, Tuple2<ControlEvent, SaleEvent>> {
        private static final long serialVersionUID = -5848671410776661036L;

        @Override
        public Tuple2<ControlEvent, SaleEvent> map(SaleEvent value) throws Exception {
            return Tuple2.of(null, value);
        }
    }

    private static class ConvertControlEvent implements MapFunction<ControlEvent, Tuple2<ControlEvent, SaleEvent>> {
        private static final long serialVersionUID = -375455730810163528L;

        @Override
        public Tuple2<ControlEvent, SaleEvent> map(ControlEvent value) throws Exception {
            return Tuple2.of(value, null);
        }
    }

    private static class DynamicFilter extends RichCoFlatMapFunction<Tuple2<ControlEvent, SaleEvent>, ProductImpressions, FilterMatch>
        implements ListCheckpointed<Filter> {

        private static final long serialVersionUID = 2754679535259430795L;
        private final Random RND = new XORShiftRandom(UUID.randomUUID().getLeastSignificantBits());
        private final int maxBlockingTime;
        private final double killLikelihood;
        private long targetNumKeys; // the operator following the dynamic filter is keyed by this.
        // so here we can control the number of rockskeys

        public DynamicFilter(ParameterTool pt) {
            this.maxBlockingTime = pt.getInt("filterMaxBlockingMs", 1500);
            this.targetNumKeys = pt.getLong("targetNumKeys", 1_000_000L);
            this.killLikelihood = pt.getDouble("killLikelihood", 0.00000001);
        }

        // registered state
        private List<Filter> currentFilters;

        @Override
        public void open(Configuration parameters) throws Exception {
            if(currentFilters == null) {
                LOG.info("Creating new filter list");
                this.currentFilters = new ArrayList<>();
            }
            getRuntimeContext().getMetricGroup().gauge("filterCount", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return currentFilters.size();
                }
            });
        }

        @Override
        public void flatMap1(Tuple2<ControlEvent, SaleEvent> value, Collector<FilterMatch> out) throws Exception {
            if(RND.nextDouble() < killLikelihood) {
                LOG.warn("Killing this TaskManager", new RuntimeException("Exception", new RuntimeException("with a cause")));
                System.exit(0);
            }

            if(value.f0 == null) {
                // we have a sale event here
                evaluateFilters(value.f1, out);
            } else if (value.f1 == null) {
                // we have a control event
                ControlEvent ctrl = value.f0;
                if(ctrl.type == ControlEvent.Type.ADD_FILTER) {
                    currentFilters.add(ctrl.filter);
                } else if (ctrl.type == ControlEvent.Type.REMOVE_FILTER) {
                    // this can take a while, and its okay :)
                    currentFilters.remove(ctrl.filter);
                }
            } else {
                throw new RuntimeException("There's an error", new RuntimeException("And the error has a cause"));
            }
        }

        private void evaluateFilters(ProductDescriptor product, Collector<FilterMatch> out) throws InterruptedException {
            /*for(Filter filter: currentFilters) {
                if(filter.match(product)) {
                    // first, this is a good opportunity to block for a while (for backpressure)
                  //  Thread.sleep(RND.nextInt(maxBlockingTime));
                }
            } */
            FilterMatch fm = new FilterMatch();
            fm.id = (long)(RND.nextDouble()*targetNumKeys);
            out.collect(fm);
        }

        @Override
        public void flatMap2(ProductImpressions value, Collector<FilterMatch> out) throws Exception {

        }

        // ---------------------- state handling ----------------------

        @Override
        public List<Filter> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            return this.currentFilters;
        }

        @Override
        public void restoreState(List<Filter> list) {
            LOG.info("Restoring filter list");
            this.currentFilters = list;
        }

    }


    private static class ElementsPerSecond {
        private final long eps;
        private long count;
        private long start;

        public ElementsPerSecond(long eps) {
            this.eps = eps;
            count = 0;
            start = System.currentTimeMillis();
        }

        public void reportElement() throws InterruptedException {
            if(++count == eps) {
                long passed = (System.currentTimeMillis() - start);
                long remaining = 1000 - passed;
                if(remaining > 0) {
                    Thread.sleep(remaining);
                }
                count = 0;
                start = System.currentTimeMillis();
            }
        }
    }


    private static class DynamicSalesFilter implements CoFlatMapFunction<ControlEvent, SaleEvent, Tuple2<ControlEvent, SaleEvent>> {
        private static final long serialVersionUID = -3284209663410861245L;

        public DynamicSalesFilter(ParameterTool pt) {

        }

        @Override
        public void flatMap1(ControlEvent value, Collector<Tuple2<ControlEvent, SaleEvent>> out) throws Exception {
            out.collect(Tuple2.of(value, (SaleEvent)null));
        }

        @Override
        public void flatMap2(SaleEvent value, Collector<Tuple2<ControlEvent, SaleEvent>> out) throws Exception {
            out.collect(Tuple2.of((ControlEvent)null, value));
        }
    }

    private static class WarehouseUpdates {
        private byte[] workload;

        public WarehouseUpdates(byte[] wl) {
            this.workload = wl;
        }
    }

    private static class WarehouseUpdatesGenerator extends RichParallelSourceFunction<WarehouseUpdates> {
        private static final long serialVersionUID = 3699608433835144607L;
        private boolean running = true;
        private long elementsPerSecond;

        public WarehouseUpdatesGenerator(ParameterTool pt) {
            elementsPerSecond = pt.getLong("WarehouseUpdatesGenerator.EPS", 50_000L);
        }

        @Override
        public void run(SourceContext<WarehouseUpdates> ctx) throws Exception {
            ElementsPerSecond perSecond = new ElementsPerSecond(elementsPerSecond);
            byte[] wl = new byte[48];
            while(running) {
                ctx.collect(new WarehouseUpdates(wl));
                perSecond.reportElement();
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class FilterInvalidItemsFromWarehouse implements CoFlatMapFunction<ControlEvent, WarehouseUpdates, Object> {
        private static final long serialVersionUID = -2390849414121020611L;

        @Override
        public void flatMap1(ControlEvent value, Collector<Object> out) throws Exception {

        }

        @Override
        public void flatMap2(WarehouseUpdates value, Collector<Object> out) throws Exception {

        }
    }
}
