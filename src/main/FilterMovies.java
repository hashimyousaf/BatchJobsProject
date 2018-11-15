import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class FilterMovies {
    public static void main(String[] args) throws Exception{
        System.out.println("This is hashim yousaf");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long, String, String>> lines =  env.readCsvFile("src/ml-latest-small/movies.csv")
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .ignoreInvalidLines()
                .types(Long.class, String.class, String.class);

        DataSet<Movie> movies =  lines.map(new MapFunction<Tuple3<Long,String,String>, Movie>() {
            public Movie map(Tuple3<Long, String, String> csvLine) {
                String movieName = csvLine.f1;
                String[] genres = csvLine.f2.split("\\|");
                return new Movie(movieName, new HashSet<String>(Arrays.asList(genres)));

            }
        });
        movies.print();
    }
}

class Movie{
    private String name;
    private Set<String> genres;

    public Movie(String name, Set<String> genres) {
        this.name = name ;
        this.genres = genres ;
    }

    public String getName() { return name;}
    public Set<String> getGenres(){ return genres; }

    @Override
    public String toString() {
        return "Movie{name='" + name + '\'' +  ", genres=" + genres + '}';
    }
}