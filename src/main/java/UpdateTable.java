import Tables.*;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.HashMap;

public class UpdateTable extends Tuple3<String, String, Object> {
    public String op;
    public String tableName;
    public Customer customerTuple;
    public Lineitem lineitemTuple;
    public Nation nationTuple;
    public Orders ordersTuple;
    public Supplier supplierTuple;
    public String any = "same";
    public Long primaryKey;
    public HashMap<String, Long> foreignKeyMap;

    public UpdateTable() {}

    // Customer
    public UpdateTable(String op, String tableName, Customer customerTuple) {
        super(op, tableName, customerTuple);
        this.op = op;
        this.tableName = tableName;
        this.customerTuple = customerTuple;
        this.primaryKey = customerTuple.getPrimaryKey();
        this.foreignKeyMap = customerTuple.getForeignKey();
    }

    // Lineitem
    public UpdateTable(String op, String tableName, Lineitem lineitemTuple) {
        super(op, tableName, lineitemTuple);
        this.op = op;
        this.tableName = tableName;
        this.lineitemTuple = lineitemTuple;
        this.primaryKey = lineitemTuple.getPrimaryKey();
        this.foreignKeyMap = lineitemTuple.getForeignKey();
    }

    // Nation
    public UpdateTable(String op, String tableName, Nation nationTuple) {
        super(op, tableName, nationTuple);
        this.op = op;
        this.tableName = tableName;
        this.nationTuple = nationTuple;
        this.primaryKey = nationTuple.getPrimaryKey();
        this.foreignKeyMap = nationTuple.getForeignKey();
    }

    // Orders
    public UpdateTable(String op, String tableName, Orders ordersTuple) {
        super(op, tableName, ordersTuple);
        this.op = op;
        this.tableName = tableName;
        this.ordersTuple = ordersTuple;
        this.primaryKey = ordersTuple.getPrimaryKey();
        this.foreignKeyMap = ordersTuple.getForeignKey();
    }

    // Supplier
    public UpdateTable(String op, String tableName, Supplier supplierTuple) {
        super(op, tableName, supplierTuple);
        this.op = op;
        this.tableName = tableName;
        this.supplierTuple = supplierTuple;
        this.primaryKey = supplierTuple.getPrimaryKey();
        this.foreignKeyMap = supplierTuple.getForeignKey();
    }

    @Override
    public String toString() {
        if (tableName.equals("Customer")) {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", customerTuple=" + customerTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
        else if (tableName.equals("Lineitem")) {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", lineitemTuple=" + lineitemTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
        else if (tableName.equals("NATION1")) {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", nationTuple=" + nationTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
        else if (tableName.equals("NATION2")) {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", nationTuple=" + nationTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
        else if (tableName.equals("Orders")) {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", ordersTuple=" + ordersTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
        else {
            return "UpdateTable{" +
                    "op='" + op + '\'' +
                    ", tableName='" + tableName + '\'' +
                    ", supplierTuple=" + supplierTuple +
                    ", primaryKey=" + primaryKey +
                    ", foreignKeyMap=" + foreignKeyMap +
                    '}';
        }
    }
}
