package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;

public class WordCount {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.readTextFile("/Users/jiaojiao/IdeaProjects/wikiedits/src/main/resources/wordcount.input");

        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);


        counts.writeAsCsv("/Users/jiaojiao/IdeaProjects/wikiedits/src/main/resources/wordcount.output", "\n", " ");

        env.execute();
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split(" ");

            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}
