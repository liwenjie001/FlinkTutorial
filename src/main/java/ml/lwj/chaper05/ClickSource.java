package ml.lwj.chaper05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

// 一般用于测试使用
public class ClickSource implements SourceFunction<Event> {
    // 声明一个标志位

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        // 随机生成数据
        Random random = new Random();
        // 定义字段选取的数据集
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                "./prod?id=2"};

        // 循环生成数据
        while (running) {
            Thread.sleep(1000L);
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user,url,timestamp));
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
