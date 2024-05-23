package Tables;

import org.apache.flink.api.java.tuple.Tuple6;

import java.sql.Date;
import java.util.HashMap;

public class Lineitem extends Tuple6<Long, Long, Long, Double, Double, Date> {
    public Long l_orderkey;
    public Long l_suppkey;
    public Long l_lineNumber;
    public Double l_extendedPrice;
    public Double l_discount;
    public Date l_shipDate;
    public Lineitem() {}

    public Lineitem(Long l_orderkey, Long l_suppkey, Long l_lineNumber, Double l_extendedPrice, Double l_discount, Date l_shipDate) {
        super(l_orderkey, l_suppkey, l_lineNumber, l_extendedPrice, l_discount, l_shipDate);
        this.l_orderkey = l_orderkey;
        this.l_suppkey = l_suppkey;
        this.l_lineNumber = l_lineNumber;
        this.l_extendedPrice = l_extendedPrice;
        this.l_discount = l_discount;
        this.l_shipDate = l_shipDate;
    }

    public Long getPrimaryKey() {
        Long ok = this.l_orderkey;
        Long ln = this.l_lineNumber;
        return (ok >= ln)? ok * ok + ok + ln : ok + ln * ln; // Some question?
    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMap = new HashMap<String, Long>();
        foreignKeyMap.put("Supplier", this.l_suppkey);
        foreignKeyMap.put("Orders", this.l_orderkey);
        return foreignKeyMap;
    }
}
