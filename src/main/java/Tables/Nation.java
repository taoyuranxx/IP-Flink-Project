package Tables;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Nation extends Tuple2<Long, String> {
    public Long n_nationkey;
    public String n_name;
    public Nation() {}

    public Nation(Long n_nationkey, String n_name) {
        super(n_nationkey, n_name);
        this.n_nationkey = n_nationkey;
        this.n_name = n_name;
    }

    public Long getPrimaryKey() {
        return this.n_nationkey;
    }

    // Nation table is a leaf table
    public HashMap<String, Long> getForeignKey() {
        return new HashMap<String, Long>();
    }
}
