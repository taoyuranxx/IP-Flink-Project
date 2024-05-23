import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Date;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * 使用Flink的DataStream API实现TPC-H Query7的执行
 * 包含从六个表（"Customer", "Lineitem", "NATION1", "NATION2", "Orders", "Supplier"）中过滤数据的步骤
 * 并基于查询条件处理这些过滤后的数据以生成结果，最终print到终端
 */
public class ExecuteQuery {
//    private static final Logger logger = Logger.getLogger(ExecuteQuery7.class.getName());
    public static void main(String[] args) throws Exception {
        // 在main方法开始时记录日志
//        logger.info("Flink process for TPC-H Query 7 started");
        int parallelism = 32; // 并行度设定
        // Query 7中的供应商国家和顾客国家设定
        String NATION1 = "CHINA";
        String NATION2 = "UNITED STATES";

        // 初始化Flink作业的执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度:1~43
        env.setParallelism(parallelism);

        // 从数据源读取输入数据
        DataStream<UpdateTable> inputStream = env.addSource(new SourceFunctionForQuery7());

        // 根据Query 7的查询条件
        // 对数据采取过滤操作
        inputStream = inputStream
                .filter((FilterFunction<UpdateTable>) data -> {
                            switch (data.tableName) {
                                case "Lineitem":
                                    // 针对Lineitem表的过滤：发货日期在1995年至1996年之间
                                    return !data.lineitemTuple.l_shipDate.before(Date.valueOf("1995-01-01")) &&
                                            !data.lineitemTuple.l_shipDate.after(Date.valueOf("1996-12-31"));
                                case "NATION1":  // (n1.n_name = '[NATION1]' or n1.n_name = '[NATION2]')
                                    // 针对Nation表的过滤：指定供应商国家和顾客国家
                                    return data.nationTuple.n_name.equals(NATION1) || data.nationTuple.n_name.equals(NATION2);
                                case "NATION2":  // (n2.n_name = '[NATION1]' or n2.n_name = '[NATION2]')
                                    return data.nationTuple.n_name.equals(NATION1) || data.nationTuple.n_name.equals(NATION2);
                            }
                    return true;
                }
                );

        // 对查询进行处理
        DataStream<List<Tuple4<String, String, Integer, Double>>> res = inputStream.keyBy("any").process(new ProcessFunctionForQuery7());

        // 不断地输出查询结果
        res.print();

        // 执行Flink作业并统计运行时间
        System.out.println("Flink process for TPC-H Query 7 started");
        final Instant start = Instant.now();
        env.execute("Flink process for TPC-H Query 7");
        final Instant finish = Instant.now();
        final Duration execDuration = Duration.between(start, finish);
        System.out.println("Flink process for TPC-H Query 7 finished in " + execDuration.getSeconds() + "s");
        // 在结束时记录日志
//        logger.info("Flink process for TPC-H Query 7 finished in " + execDuration.getSeconds() + "s");
}

}
