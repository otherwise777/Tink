package Tgraphs;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;

/**
 * Class for doing some benchmarking tests with the EAT algorithm
 * The class iterates over the different graphs indicated by @param graphs
 * the class then appends the results in the results.txt file for the results
 *
 */
public class analyze_rita {
    public static void main(String[] args) throws Exception {

        /*
        Define some objects like input output files.
         */
        final ExecutionEnvironment env;
        env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        int maxiterations = 30;
        String output = "C:\\Users\\ia02ui\\IdeaProjects\\Temporal_Graph_library\\datasets\\results.txt";
        String input = "..\\..\\..\\..\\datasets\\rita_data.csv";
        input = "C:\\Users\\ia02ui\\IdeaProjects\\Temporal_Graph_library\\datasets\\rita_data.csv";

        /*
        * create a Flink DataSet tuple 6 and import the data from the csv file.
        * with .includeFields("0100000100010010010001") we indicate that we want the 2nd, 8th column etc..
        * The types define the parsing type for importing.
        * */
        DataSet<Tuple6<Double, String, String, Double, Double, Double>> flightSet = env.readCsvFile(input)
                .fieldDelimiter(",")  // node IDs are separated by spaces
                .ignoreComments("%")  // comments start with "%"
                .includeFields("0100000100010010010001")
                .types(Double.class, String.class, String.class, Double.class, Double.class, Double.class); // read the node IDs as Longs

        /*
        * Since the data is not formatted in the way we want we map it to a dataset that we can use for the graph in the format
        * (Source id, Target id, starting time, ending time)
        *
        * */
        DataSet<Tuple4<String, String, Double, Double>> flightSetMapped = flightSet.map(new MapFunction<Tuple6<Double, String, String, Double, Double, Double>, Tuple4<String, String, Double, Double>>() {
            @Override
            public Tuple4<String, String, Double, Double> map(Tuple6<Double, String, String, Double, Double, Double> v) throws Exception {
                Double starttime = v.f0 + ((v.f3 / 100) * 60  + v.f3 % 100) * 60;
                Double endtime = v.f0 + ((v.f4 / 100) * 60  + v.f4 % 100) * 60;
                return new Tuple4<>(v.f1,v.f2,starttime ,endtime);
            }
        });
//        flightSetMapped.first(10).writeAsText(output);

        DataSet<Tuple4<String, String, Long, Long>> flightSetMapped2 = flightSetMapped.map(new MapFunction<Tuple4<String, String, Double, Double>, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> map(Tuple4<String, String, Double, Double> v) throws Exception {
                return new Tuple4<>(v.f0,v.f1,v.f2.longValue(),v.f3.longValue());
            }
        });
//        flightSetMapped2.first(1).print();
        Tgraph<String, NullValue, NullValue, Double> Rita_graph = Tgraph.From4TupleNoEdgesNoVertexes(flightSetMapped, env);
////

        System.out.println(Rita_graph.run(new SSSTPClosenessSingleNode<>("JFK",maxiterations,1,false)));
//        DataSet<Tuple3<String, Integer,ArrayList<String>>> t = Rita_graph.getUndirected().run(new SingleSourceShortestTemporalPathEATJustPaths<>("JFK", maxiterations))
//                .map(new MapFunction<Vertex<String, ArrayList<String>>, Tuple3<String, Integer,ArrayList<String>>>() {
//                    @Override
//                    public Tuple3<String, Integer, ArrayList<String>> map(Vertex<String, ArrayList<String>> v) throws Exception {
//                        return new Tuple3<>(v.f0,v.f1.size(),v.f1);
//                    }
//                });
//        t.sortPartition(1, Order.DESCENDING).first(100).print();
    }
}
