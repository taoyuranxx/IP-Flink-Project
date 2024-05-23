package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Supplier extends Tuple2<Long, Long> {
    public Long s_suppkey;
    public Long s_nationkey;
    public Supplier() {}

    public Supplier(Long s_suppkey, Long s_nationkey) {
        super(s_suppkey, s_nationkey);
        this.s_suppkey = s_suppkey;
        this.s_nationkey = s_nationkey;
    }

    public Long getPrimaryKey() {
        return this.s_suppkey;
    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMap = new HashMap<String, Long>();
        foreignKeyMap.put("NATION1", this.s_nationkey);
        return foreignKeyMap;
    }
}
