package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Customer extends Tuple2<Long, Long> {
    public Long c_custkey;
    public Long c_nationkey;
    public Customer() {}

    public Customer(Long c_custkey, Long c_nationkey) {
        super(c_custkey, c_nationkey);
        this.c_custkey = c_custkey;
        this.c_nationkey = c_nationkey;
    }

    public Long getC_custkey() {
        return c_custkey;
    }

    public Long getC_nationkey() {
        return c_nationkey;
    }

    public Long getPrimaryKey() {
        return c_custkey;
    }

//    public Long getForeignKey() {
//        return c_nationkey;
//    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMap = new HashMap<String, Long>();
        foreignKeyMap.put("NATION2", this.c_nationkey);
        return foreignKeyMap;
    }
}
