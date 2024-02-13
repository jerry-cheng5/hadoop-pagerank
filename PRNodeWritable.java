
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Writable;

/**
 * PRNodeWritable
 */
public class PRNodeWritable implements Writable {

    private int nodeID;
    private List<Integer> adjacencyList;
    private double prValue;

    public PRNodeWritable() {}
    public PRNodeWritable(PRNodeWritable n) {
        this.nodeID = n.getNodeID();
        this.adjacencyList = n.getAdjacencyList();
        this.prValue = n.getPRValue();
    } 

    public PRNodeWritable(int id, List<Integer> aList, double pr) {
        this.nodeID = id;
        this.adjacencyList = aList;
        this.prValue = pr;
    }

    // For passing PR value only
    public PRNodeWritable(double pr) {
        this.nodeID = 0;
        this.adjacencyList = new ArrayList<>();
        this.prValue = pr;
    }

    public void setNodeID(int id) { nodeID = id; }
    public void setAdjacencyList(List<Integer> aList) { adjacencyList = aList; }
    public void setPRValue(double value) { prValue = value; }

    public int getNodeID() { return nodeID; }
    public List<Integer> getAdjacencyList() { return adjacencyList; }
    public double getPRValue() { return prValue; }

    public String toString() {
        String ret = Double.toString(prValue);
        for (int v : adjacencyList) {
            ret += (' ' + Integer.toString(v));
        }
        return ret.trim();
    }

    public void fromString(String s) {
        StringTokenizer itr = new StringTokenizer(s);

        // discard first int
        // itr.nextToken();

        nodeID = Integer.parseInt(itr.nextToken());
        prValue = Double.parseDouble(itr.nextToken());

        List<Integer> aList = new ArrayList<Integer>();
        while (itr.hasMoreTokens()) {
            aList.add(Integer.parseInt(itr.nextToken()));
        }
        adjacencyList = aList;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodeID);

        out.writeInt(adjacencyList.size());
        for (int v : adjacencyList) {
            out.writeInt(v);
        }

        out.writeDouble(prValue);
    }

    public void readFields(DataInput in) throws IOException {
        nodeID = in.readInt();

        int size = in.readInt();
        adjacencyList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            adjacencyList.add(in.readInt());
        }

        prValue = in.readDouble();
    }

    public static PRNodeWritable read(DataInput in) throws IOException {
        PRNodeWritable n = new PRNodeWritable();
        n.readFields(in);
        return n;
    }

}
