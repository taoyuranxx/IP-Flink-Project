import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

public class TableManager {
    public String tableName;
    public int numOfChildren;
    public boolean isLeaf;
    public boolean isRoot;
    public String parentTableName;
    public ArrayList<String> childTableNames;
    public Hashtable<Long, UpdateTable> Tuples;
    public Hashtable<Long, UpdateTable> idxOfLiveTuple; // I(L(R)) leaf R, (key,value) = (pKey, live tuple)
    public Hashtable<Long, UpdateTable> idxOfDeadTuple; // I(N(R)) non-leaf R, (key,value) = (pKey, non-live tuple)
    public Hashtable<Long, Integer> numOfAliveChildren; // for non-leaf R, (key,value) = (pKey, num of child of this tuple is alive)
    public Hashtable<String, HashMap<Long, ArrayList<Long>>> idxOfTableAndChildTableNames; //(key, value) = childName, thisTableAndTableChildInfo
    public TableManager() {}

    public TableManager(String tableName) {
        this.tableName = tableName;
    }

    public void initialize(int numOfChildren, boolean isLeaf, boolean isRoot, String parentTableName, ArrayList<String> childTableNames, Hashtable<Long, UpdateTable> tuples, Hashtable<Long, UpdateTable> idxOfLiveTuple, Hashtable<Long, UpdateTable> idxOfDeadTuple, Hashtable<Long, Integer> numOfAliveChildren, Hashtable<String, HashMap<Long, ArrayList<Long>>> idxOfTableAndChildTableNames) {
        this.numOfChildren = numOfChildren;
        this.isLeaf = isLeaf;
        this.isRoot = isRoot;
        this.parentTableName = parentTableName;
        this.childTableNames = childTableNames;
        this.Tuples = tuples;
        this.idxOfLiveTuple = idxOfLiveTuple;
        this.idxOfDeadTuple = idxOfDeadTuple;
        this.numOfAliveChildren = numOfAliveChildren;
        this.idxOfTableAndChildTableNames = idxOfTableAndChildTableNames;
    }

    public Hashtable<Long, Integer> getNumOfAliveChildren() {
        return this.numOfAliveChildren;
    }

    @Override
    public String toString() {
        return "TableManager{" +
                "tableName='" + tableName + '\'' +
                ", numOfChildren=" + numOfChildren +
                ", isLeaf=" + isLeaf +
                ", isRoot=" + isRoot +
                ", parentTableName='" + parentTableName + '\'' +
                ", childTableNames=" + childTableNames +
                ", Tuples=" + Tuples +
                ", idxOfLiveTuple=" + idxOfLiveTuple +
                ", idxOfDeadTuple=" + idxOfDeadTuple +
                ", numOfAliveChildren=" + numOfAliveChildren +
                ", idxOfTableAndChildTableNames=" + idxOfTableAndChildTableNames +
                '}';
    }
}
