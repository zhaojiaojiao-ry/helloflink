package wikiedits;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception{
        // can be used to set execution parameters and create sources for reading from external systems
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a source that reads from the Wikipedia IRC log
        DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        // gives us a Stream of WikipediaEditEvent that has a String key, the user name
        KeyedStream<WikipediaEditEvent, String> keyedEdits =  edits
                .keyBy(new KeySelector<WikipediaEditEvent, String>() {
                    public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                        return wikipediaEditEvent.getUser();
                    }
                });

        // windowing of 5 seconds, result is a tuple of 2 elements, the first element is user name, the second element is edit bytes
        DataStream<Tuple2<String, Long>> result = keyedEdits
                .timeWindow(Time.seconds(5))
                .fold(new Tuple2<String, Long>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
                    public Tuple2<String, Long> fold(Tuple2<String, Long> stringLongTuple2, WikipediaEditEvent o) throws Exception {
                        stringLongTuple2.f0 = o.getUser();
                        stringLongTuple2.f1 += o.getByteDiff();
                        return stringLongTuple2;
                    }
                });

        // output the stream to console, add a sink std out
        result.print();

        // start execution
        see.execute();
    }
}
