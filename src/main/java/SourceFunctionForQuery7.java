import Tables.*;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Date;
import java.util.Scanner;
import java.io.File;

public class SourceFunctionForQuery7 implements SourceFunction<UpdateTable> {
    String dataPath = "DemoTools/DataGenerator/source_data.csv";
    boolean flag = true;

    @Override
    public void run(SourceContext<UpdateTable> sourceContext) throws Exception {
        Scanner scanner = new Scanner(new File(dataPath));

        while (flag) {
            String[] line = scanner.nextLine().split("\\|");
            String op = line[0];
            String tableName = line[1];
            if (tableName.equals("customer.tbl")) {
                Customer newTuple = new Customer(Long.valueOf(line[2]), Long.valueOf(line[5]));
                sourceContext.collect(new UpdateTable(op, "Customer", newTuple));
            }
            else if (tableName.equals("lineitem.tbl")) {
                Lineitem newTuple = new Lineitem(Long.valueOf(line[2]), Long.valueOf(line[4]), Long.valueOf(line[5]), Double.valueOf(line[7]), Double.valueOf(line[8]), Date.valueOf(line[12]));
                sourceContext.collect(new UpdateTable(op, "Lineitem", newTuple));
            }
            else if (tableName.equals("nation.tbl")) {
                Nation newTuple = new Nation(Long.valueOf(line[2]), line[3]);
                sourceContext.collect(new UpdateTable(op, "NATION1", newTuple));
                sourceContext.collect(new UpdateTable(op, "NATION2", newTuple));
            }
            else if (tableName.equals("orders.tbl")) {
                Orders newTuple = new Orders(Long.valueOf(line[2]), Long.valueOf(line[3]));
                sourceContext.collect(new UpdateTable(op, "Orders", newTuple));
            }
            else if (tableName.equals("supplier.tbl")) {
                Supplier newTuple = new Supplier(Long.valueOf(line[2]), Long.valueOf(line[5]));
                sourceContext.collect(new UpdateTable(op, "Supplier", newTuple));
            }

            if(!scanner.hasNext()) {
                flag = false;
                System.out.println("Data source has been read completely!");
            }
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
