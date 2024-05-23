package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Orders extends Tuple2<Long, Long> {
    public Long o_orderkey;
    public Long o_custkey;
    public Orders() {}

    public Orders(Long o_orderkey, Long o_custkey) {
        super(o_orderkey, o_custkey);
        this.o_orderkey = o_orderkey;
        this.o_custkey = o_custkey;
    }

    public Long getPrimaryKey() {
        return this.o_orderkey;
    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMap = new HashMap<String, Long>();
        foreignKeyMap.put("Customer", this.o_custkey);
        return foreignKeyMap;
    }
}
