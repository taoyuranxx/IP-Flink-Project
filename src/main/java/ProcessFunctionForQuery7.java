import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.*;

import java.util.*;

// 处理数据流上的更新操作，维护和更新查询处理所需要的状态
// 包括初始化表状态，处理插入和删除操作，以及根据join的结果执行计算
public class ProcessFunctionForQuery7 extends ProcessFunction<UpdateTable, List<Tuple4<String, String, Integer, Double>>> {
    // 用于存储join结果的状态(Integer->Double)
    private MapState<Long, Tuple4<String, String, Integer, Double>> joinState;
    // 存储所有表的信息
    private MapState<String, TableManager> tableState;
    // 存储tuple的计算浮点数结果
    private MapState<Tuple3<String, String, Integer>, Double> calResult;

    // 判断是否首次运行，如果是则初始化表信息
    // 执行插入和删除逻辑
    // 根据join结果进行计算并更新calculationState
    // 收集最终计算结果并清空临时计算状态
    @Override
    public void processElement(UpdateTable updateTable, Context context,
                               Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // 初始化所有表的信息
        if (tableState.isEmpty()) {
            System.out.println("Start to initialize the log of tables");
            TableManager customerTable = new TableManager("Customer");
            TableManager lineitemTable = new TableManager("Lineitem");
            TableManager nation1Table = new TableManager("NATION1");
            TableManager nation2Table = new TableManager("NATION2");
            TableManager ordersTable = new TableManager("Orders");
            TableManager supplierTable = new TableManager("Supplier");

            // Customer table
            customerTable.initialize(1,false, false, "Orders", new ArrayList<>(Arrays.asList("NATION2")),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Lineitem table
            lineitemTable.initialize(2,false, true, "", new ArrayList<>(Arrays.asList("Orders", "Supplier")),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // NATION1 table
            nation1Table.initialize(0,true, false, "Supplier", new ArrayList<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // NATION2 table
            nation2Table.initialize(0,true, false, "Customer", new ArrayList<>(),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Orders table
            ordersTable.initialize(1,false, false, "Lineitem", new ArrayList<>(Arrays.asList("Customer")),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            // Supplier table
            supplierTable.initialize(1,false, false, "Lineitem", new ArrayList<>(Arrays.asList("NATION1")),
                    new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>(), new Hashtable<>());

            tableState.put("Customer", customerTable);
            tableState.put("Lineitem", lineitemTable);
            tableState.put("NATION1", nation1Table);
            tableState.put("NATION2", nation2Table);
            tableState.put("Orders", ordersTable);
            tableState.put("Supplier", supplierTable);
        }

        insert(tableState, updateTable, collector);
        delete(tableState, updateTable, collector);

        // 遍历更新
        Iterable<Tuple4<String, String, Integer, Double>> joinResult = joinState.values();
        for (Tuple4<String, String, Integer, Double> tupleResult : joinResult) {
            if (!tupleResult.f0.equals(tupleResult.f1)) {
                Tuple3<String, String, Integer> keyBy = new Tuple3<>(tupleResult.f0, tupleResult.f1, tupleResult.f2);
                if (calResult.get(keyBy) != null) {
                    double oldValue = calResult.get(keyBy);
                    calResult.put(keyBy, oldValue + tupleResult.f3);
                } else {
                    calResult.put(keyBy, tupleResult.f3);
                }
            }
        }

        // calResult中的数据转换成Tuple，并存储到output列表中
        // 然后通过collector将output收集起来，最后清空calResult
        List<Tuple4<String, String, Integer, Double>> output = new ArrayList<>();
        for (Map.Entry<Tuple3<String, String, Integer>, Double> entry : calResult.entries()) {
            Tuple3<String, String, Integer> key = entry.getKey();
            Double value = entry.getValue();
            output.add(new Tuple4<>(key.f0, key.f1, key.f2, value));
        }
        collector.collect(output);
        calResult.clear();
    }

    // 在函数打开时初始化各个状态描述符 为MapState分配内存和类型信息
    @Override
    public void open(Configuration parameters) throws Exception {
        joinState = getRuntimeContext().getMapState(new MapStateDescriptor<>("ResultState", Types.LONG, Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE)));
        tableState = getRuntimeContext().getMapState(new MapStateDescriptor<>("AllTableState", String.class, TableManager.class));
        calResult = getRuntimeContext().getMapState(new MapStateDescriptor<>("CalResult", Types.TUPLE(Types.STRING, Types.STRING, Types.INT), Types.DOUBLE));
    }

    // Insert算法
    private void insert(MapState<String, TableManager> tableState, UpdateTable updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        if (updateTuple.op.equals("+")) {
            TableManager tupleTable = tableState.get(updateTuple.tableName);
            tupleTable.Tuples.put(updateTuple.primaryKey, updateTuple);
            if (!tupleTable.isLeaf) {
                // s <- 0 初始化tuple计数器, 用主键做索引
                Hashtable<Long, Integer> numOfAliveChildren = tupleTable.numOfAliveChildren;
                numOfAliveChildren.put(updateTuple.primaryKey, 0);
                // 对每一个 Rc ∈ C(R)
                for (String childTableName : tupleTable.childTableNames) {
                    //I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
                    // 初始化 I(R,Rc) （如果为null）
                    tupleTable.idxOfTableAndChildTableNames.computeIfAbsent(childTableName, k -> new HashMap<Long, ArrayList<Long>>());
                    Long tupleForeignKey = updateTuple.foreignKeyMap.get(childTableName);
                    // 如果不存在键值 初始化arraylist
                    ArrayList<Long> lst = tupleTable.idxOfTableAndChildTableNames.get(childTableName).get(tupleForeignKey);
                    if (lst != null) {
                        if (lst.contains(updateTuple.primaryKey)) {
                            throw new Exception("Wrong primary key!");
                        }
                        lst.add(updateTuple.primaryKey);
                    } else {
                        tupleTable.idxOfTableAndChildTableNames.get(childTableName).put(tupleForeignKey, new ArrayList<>(Arrays.asList(updateTuple.primaryKey)));
                    }
                    // 如果 πPK(Rc)t ∈ I(Rc) 则 s(t) ← s(t) + 1
                    if (tableState.get(childTableName).idxOfLiveTuple.containsKey(tupleForeignKey)) {
                        numOfAliveChildren.compute(updateTuple.primaryKey, (k, oldValue) -> oldValue + 1);
                    }
                }
                // Query 7无断言键 不需要更新断言键
            }
            // 如果 R is a leaf or s(t) = |C(R)|
            if (tupleTable.isLeaf || (tupleTable.getNumOfAliveChildren().get(updateTuple.primaryKey) == Math.abs(tupleTable.numOfChildren))) {
                // insert-Update(t, R,t)
                insertUpdate(tableState, updateTuple, collector);
            } else { // 否则 I(N(R)) ← I(N(R)) + (πPK(R)t → t)  把这个tuple放入dead tuple集合中
                tupleTable.idxOfDeadTuple.put(updateTuple.primaryKey, updateTuple);
            }
        }
    }

    // Insert-Update算法
    public void insertUpdate(MapState<String, TableManager> tableState, UpdateTable updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // I(L(R)) ← I(L(R)) + (πPK(R)t → t)
        // 放入live tuple集合
        TableManager tupleTable = tableState.get(updateTuple.tableName);
        tupleTable.idxOfLiveTuple.put(updateTuple.primaryKey, updateTuple);
        // 如果R是根的话，将join结果放入join state中
        if (tupleTable.isRoot && (tupleTable.numOfAliveChildren.get(updateTuple.primaryKey) == tupleTable.numOfChildren)) {
            // 执行join操作
            Tuple4<String, String, Integer, Double> res = getSelectedTuple(tableState, updateTuple.primaryKey);
            joinState.put(updateTuple.primaryKey, res);
        } else {
            // 否则查表
            TableManager parentTable = tableState.get(tupleTable.parentTableName);
            HashMap<Long, ArrayList<Long>> I = parentTable.idxOfTableAndChildTableNames.get(updateTuple.tableName);
            if (I != null) {
                ArrayList<Long> parentPrimaryKey = I.get(updateTuple.primaryKey);
                //对每一个 tp ∈ P
                if (parentPrimaryKey != null) {
                    for (Long eachKey : parentPrimaryKey) {
                        // s(tp ) ← s(tp ) + 1
                        parentTable.numOfAliveChildren.compute(eachKey, (k, oldValue) -> oldValue + 1);

                        // 如果 s(tp ) = |C(Rp )|
                        if (parentTable.numOfAliveChildren.get(eachKey) == Math.abs(parentTable.numOfChildren)) {
                            // I(N(Rp )) ← I(N(Rp )) − (πPK(Rp ) tp → tp ) 更新dead tuple集合
                            if (parentTable.Tuples.containsKey(eachKey)) {
                                UpdateTable parentTuple = parentTable.Tuples.get(eachKey);
                                parentTable.idxOfDeadTuple.remove(eachKey);
                                // Insert-Update(tp, Rp, join_result tp)
                                insertUpdate(tableState, parentTuple, collector);
                            }
                        }
                    }
                }
            }
        }
    }

    // Delete算法
    public void delete(MapState<String, TableManager> tableState, UpdateTable updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        if (updateTuple.op.equals("-")) {
            TableManager tupleTable = tableState.get(updateTuple.tableName);
            tupleTable.Tuples.remove(updateTuple.primaryKey);
            // 如果 t ∈ L(R) 则看它是不是在live tuple集合里
            if (tupleTable.idxOfLiveTuple.containsKey(updateTuple.primaryKey)) {
                deleteUpdate(tableState, updateTuple, collector);
            } else {
                // 从dead tuple集合中移除这个tuple
                tupleTable.idxOfDeadTuple.remove(updateTuple.primaryKey);
            }

            // 如果不是根表
            if (!tupleTable.isRoot) {
                // I(Rp, R) ← I(Rp, R) − (πPK(R)t → πPK(Rp ),PK(R)t)
                TableManager parentTable = tableState.get(tupleTable.parentTableName);
                HashMap<Long, ArrayList<Long>> I = parentTable.idxOfTableAndChildTableNames.get(updateTuple.tableName);
                if (I != null) {
                    I.remove(updateTuple.primaryKey);
                }
            }
        }
    }

    // Delete-Update算法
    public void deleteUpdate(MapState<String, TableManager> tableState, UpdateTable updateTuple, Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // remove from live tuple
        TableManager tupleTable = tableState.get(updateTuple.tableName);
        tupleTable.idxOfLiveTuple.remove(updateTuple.primaryKey);
        if (tupleTable.isRoot) {
            // 从join state中删除
            joinState.remove(updateTuple.primaryKey);
        } else {
            // 查表
            TableManager parentTable = tableState.get(tupleTable.parentTableName);
            HashMap<Long, ArrayList<Long>> I = parentTable.idxOfTableAndChildTableNames.get(updateTuple.tableName);
            if (I != null) {
                ArrayList<Long> parentPrimaryKey = I.get(updateTuple.primaryKey);
                // 对每一个 tp ∈ P
                if (parentPrimaryKey != null) {
                    for (Long eachKey : parentPrimaryKey) {
                        // 如果tp ∈ N(Rp)
                        if (parentTable.idxOfDeadTuple.containsKey(eachKey)) {
                            // s(tp) ← s(tp) - 1
                            parentTable.numOfAliveChildren.compute(eachKey, (k, oldValue) -> oldValue - 1);
                        } else {
                            // s(tp) ← |C(R)| − 1
                            parentTable.numOfAliveChildren.put(eachKey, Math.abs(tupleTable.numOfChildren)-1);
                            //I(N(Rp)) ← I(N(Rp)) + (πPK(Rp)t → t) 更新dead tuple集合
                            if (parentTable.Tuples.containsKey(eachKey)) {
                                UpdateTable parentTuple = parentTable.Tuples.get(eachKey);
                                parentTable.idxOfDeadTuple.put(eachKey, parentTuple);
                                deleteUpdate(tableState, parentTuple, collector);
                            }
                        }
                    }
                }
            }
        }
    }

    // 从tableState中获取与LineitemPrimaryKey对应的TableManager对象，
    // 提取出订单号(l_orderkey)、供应商号(l_suppkey)、发货日期(l_shipDate)、扩展价格(l_extendedPrice)和折扣(l_discount)
    // 根据发货日期计算出发货年份(l_shipYear)和收入(volumn)
    // 最后根据供应商所在的国家名称(NATION1)、客户所在的国家名称(NATION2)、发货年份和收入，返回一个Tuple4对象
    public Tuple4<String, String, Integer, Double> getSelectedTuple(MapState<String, TableManager> tableState, Long LineitemPrimaryKey) throws Exception {
        Calendar calendar = Calendar.getInstance();
        Long l_orderkey = tableState.get("Lineitem").idxOfLiveTuple.get(LineitemPrimaryKey).lineitemTuple.l_orderkey;
        Long l_suppkey = tableState.get("Lineitem").idxOfLiveTuple.get(LineitemPrimaryKey).lineitemTuple.l_suppkey;
        Date l_shipDate = tableState.get("Lineitem").idxOfLiveTuple.get(LineitemPrimaryKey).lineitemTuple.l_shipDate;
        calendar.setTime(l_shipDate);
        int l_shipYear = calendar.get(Calendar.YEAR);
        double l_extendedPrice = tableState.get("Lineitem").idxOfLiveTuple.get(LineitemPrimaryKey).lineitemTuple.l_extendedPrice;
        double l_discount = tableState.get("Lineitem").idxOfLiveTuple.get(LineitemPrimaryKey).lineitemTuple.l_discount;
        Long s_nationkey = tableState.get("Supplier").idxOfLiveTuple.get(l_suppkey).supplierTuple.s_nationkey;
        String nation1 = tableState.get("NATION1").idxOfLiveTuple.get(s_nationkey).nationTuple.n_name;
        Long o_custkey = tableState.get("Orders").idxOfLiveTuple.get(l_orderkey).ordersTuple.o_custkey;
        Long c_nationkey = tableState.get("Customer").idxOfLiveTuple.get(o_custkey).customerTuple.c_nationkey;
        String nation2 = tableState.get("NATION2").idxOfLiveTuple.get(c_nationkey).nationTuple.n_name;
        Double volume = l_extendedPrice * (1 - l_discount); // 收入
        return Tuple4.of(nation1, nation2, l_shipYear, volume);
    }
}
