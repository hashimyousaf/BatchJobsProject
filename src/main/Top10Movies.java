import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Get top 10 movies from a movie ratings dataset.
 */
public class Top10Movies {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Double>> sorted = env.readCsvFile("src/ml-latest-small/ratings.csv")
                .ignoreFirstLine()
                .includeFields(false, true, true, false)
                .types(Integer.class, Double.class)// movieId and rating
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                    public void reduce(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Integer movieId = null;
                        double total = 0;
                        int count = 0;
                        for (Tuple2<Integer, Double> value : values) {
                            movieId = value.f0;
                            total += value.f1;
                            count++;
                        }

                        if (count > 50) {
                            out.collect(new Tuple2<Integer, Double>(movieId, total / count));
                        }

                    }
                })
                .partitionCustom(new Partitioner<Double>() {
                    public int partition(Double key, int numPartitions) {
                        return key.intValue() % numPartitions;
                    }
                }, 1)
                .setParallelism(5)
                .sortPartition(1, Order.DESCENDING)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                    public void mapPartition(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Iterator<Tuple2<Integer, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                })
                .sortPartition(1, Order.DESCENDING)
                .setParallelism(1)
                .mapPartition(new MapPartitionFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>>() {

                    public void mapPartition(Iterable<Tuple2<Integer, Double>> values, Collector<Tuple2<Integer, Double>> out) throws Exception {
                        Iterator<Tuple2<Integer, Double>> iter = values.iterator();
                        for (int i = 0; i < 10 && iter.hasNext(); i++) {
                            out.collect(iter.next());
                        }
                    }
                });

        DataSet<Tuple2<Integer, String>> movies = env.readCsvFile("src/ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .includeFields(true, true, false)
                .types(Integer.class, String.class); // movieId and title

        System.out.println("This is sorted list of movies from 5 partitions");
        System.out.println("====================================================================================");
        sorted.print();
//        System.out.println("This is Top 10 movies in list");
//        System.out.println("====================================================================================");
//        movies.first(10).print();

        DataSet<Tuple3<Integer, String, Double>> result = movies.join(sorted)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, Double>, Tuple3<Integer, String, Double>>() {

                    public Tuple3<Integer, String, Double> join(Tuple2<Integer, String> first, Tuple2<Integer, Double> second) throws Exception {
                        Integer movieId = first.f0;
                        String movieName = first.f1;
                        Double ratingOfMovie = second.f1;
                        return new Tuple3<Integer, String, Double>(movieId, movieName, ratingOfMovie);
                    }
                }).sortPartition(2, Order.DESCENDING);
        System.out.println("Here we go with our result of Top 10 rated Movies");
        System.out.println("====================================================================================");
        result.print();
    }
}


//import org.apache.flink.api.common.functions.GroupReduceFunction;
//import org.apache.flink.api.common.functions.JoinFunction;
//import org.apache.flink.api.common.functions.MapPartitionFunction;
//import org.apache.flink.api.common.functions.Partitioner;
//import org.apache.flink.api.common.operators.Order;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.util.Collector;
//
//import java.util.Iterator;
//import java.util.List;
//
//public class Top10Movies {
//    public static void main(String[] args) throws Exception{
//        // A LocalEnvironment will cause execution in the current JVM, a RemoteEnvironment will cause execution on a remote setup
//        //The environment provides methods to control the job execution (such as setting the parallelism) and
//        // to interact with the outside world (data access)
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        //DataSet programs in Flink are regular programs that implement transformations on data sets (e.g., filtering, mapping, joining, grouping).
//        // The data sets are initially created from certain sources(e.g., by reading files, or from local collections).
//        DataSet<Tuple2<Long, Double>> sorted = env.readCsvFile("src/ml-latest-small/ratings.csv")
//                .ignoreFirstLine()
//                .includeFields(false, true, true, false)
//                .types(Long.class, Double.class)
//                .groupBy(0)//group ratings by id
//                // and find an average rating for each group
//                .reduceGroup(new GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//
//                    public void reduce(Iterable<Tuple2<Long, Double>> iterable,
//                                       Collector<Tuple2<Long, Double>> collector) throws Exception {
//                        Long movieId = null;
//                        double total = 0;
//                        // and iterate through all movies
//                        int count = 0;
//                        for (Tuple2<Long, Double> value : iterable) {
//                            movieId = value.f0;
//                            total += value.f1;
//                            count++;
//                        }
//                        if (count > 50) {
//                            collector.collect(new Tuple2<Long, Double>(movieId, total / count));
//                        }
//                    }
//                    // Next thing is to split movies into different task managers. so that e
//                }).partitionCustom(new Partitioner<Double>() {
//                    // decide which partion to assign each element, and key to partition DataSet by
//                    public int partition(Double key, int numPartition) {
//                        return key.intValue() - 1;
//                    }
//                }, 1)
//                // to specify how many tasks in parallel should execute in a single step we will use setParallelism method.since
//                // movies have rating 1 to 5 we will pass it 5 execution in single task
//                .setParallelism(5)
//                // next step to sort local data on every partition.
//                .sortPartition(1, Order.DESCENDING)
//                // Now we need to get top 10 moves in every partition for that we'll use mapPartition method.
//                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    public void mapPartition(Iterable<Tuple2<Long, Double>> iterable,
//                                             Collector<Tuple2<Long, Double>> collector) throws Exception {
//                        // iterate through movies iterator and out pur first 10 using collector object
//                        Iterator<Tuple2<Long, Double>> iter = iterable.iterator();
//                        for (int i = 10; i < 10 && iter.hasNext(); i++) {
//                            collector.collect(iter.next());
//                        }
//                        // now we have top 10 movies rated 4, rated 3 and rated 2 , rated 1 and rated 5.
//                    }
//                })
//                // To sort result movies, we will use the same sort function
//                .sortPartition(1, Order.DESCENDING)
//                // Now we will specify that remining data should be sorted on single partition.
//                // As before we wil use the setParallelism method to sort this.
//                // then flink will send all remaining item to single task manager and will sort them locally.
//                .setParallelism(1)
//
//                // at this stage will will have top 50 movies  since we want just top 10 movies.we can use
//                // the same code as before to get the first 10 movies.
//                .mapPartition(new MapPartitionFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    public void mapPartition(Iterable<Tuple2<Long, Double>> iterable,
//                                             Collector<Tuple2<Long, Double>> collector) throws Exception {
//                        // iterate through movies iterator and out pur first 10 using collector object
//                        Iterator<Tuple2<Long, Double>> iter = iterable.iterator();
//                        for (int i = 10; i < 10 && iter.hasNext(); i++) {
//                            collector.collect(iter.next());
//                        }
//                        // now we have top 10 movies rated 4, rated 3 and rated 2 , rated 1 and rated 5.
//                    }
//                });
//        sorted.print();
//        // To get movie names and genres we will make use of join.
////        DataSet<Tuple2<Long, String>> movie = env.readCsvFile("src/ml-latest-small/movies.csv")
////                .ignoreFirstLine()
////                .parseQuotedStrings('"')
////                .ignoreInvalidLines()
////                .types(Long.class, String.class);
//
//        // Need to understand it completely
//
////        movie.join(sorted)
////                .where(0)
////                .equalTo(0)
////                // Join it by the result dataset by the movieId field.
////                .with(new JoinFunction<Tuple2<Long, String>, Tuple2<Long, Double>, Tuple3<Long, String, Double>>() {
////
////                    public Tuple3<Long, String, Double> join(Tuple2<Long, String> movie, Tuple2<Long, Double> rating) throws Exception{
////                        return new Tuple3<Long, String, Double>(movie.f0, movie.f1, rating.f1);
////                    }
////
////                }).print();
//
//
//    }
//}
