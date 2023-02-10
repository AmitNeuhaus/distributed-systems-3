import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TupleWritable implements WritableComparable<TupleWritable> {
    private Text first;
    private Text second;

    public TupleWritable() {
        this.first = new Text();
        this.second = new Text();
    }

    public TupleWritable(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(TupleWritable o) {
        int cmp = first.compareTo(o.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(o.second);
    }

    @Override
    public String toString(){
        return getFirst().toString() + " " +  getSecond().toString();
    }
}
