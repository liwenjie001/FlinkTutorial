package ml.lwj.chaper05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import scala.collection.Parallel;

import java.util.Random;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        DataStreamSource<Event> streamSource = env.addSource(new ClickSource());

        DataStreamSource<Integer> integerDataStreamSource = env.addSource(new ParallelCustomSource()).setParallelism(2);
//        streamSource.print();
        integerDataStreamSource.print();
        env.execute();
    }
    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {

        private Boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
