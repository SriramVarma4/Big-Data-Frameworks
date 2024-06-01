import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class ShortestPathsFlink {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> edges = env.readCsvFile(params.get("input"))
                .fieldDelimiter("\t")
                .types(Integer.class, Integer.class);

        DataSet<Tuple2<Integer, Integer>> initialDistances = edges
                .map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                        return new Tuple2<>(value.f0, Integer.MAX_VALUE);
                    }
                });

        DataSet<Tuple2<Integer, Integer>> finalDistances = initialDistances.iterate(params.getInt("iterations", 10))
                .join(edges)
                .where(0)
                .equalTo(0)
                .with(new UpdateDistance());
        finalDistances.print();

        env.execute("Shortest Paths Flink");
    }

    public static class UpdateDistance extends RichMapFunction<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> value) throws Exception {
            int source = value.f0.f0;
            int distance = value.f0.f1;
            int target = value.f1.f1;

            return new Tuple2<>(target, Math.min(distance + 1, value.f1.f0));
        }
    }
}
