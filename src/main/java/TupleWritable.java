import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TupleWritable implements Writable {
    private Text value1;
    private Text value2;

    public TupleWritable() {
        value1 = new Text();
        value2 = new Text();
    }

    public TupleWritable(String value1, String value2) {
        this.value1 = new Text(value1);
        this.value2 = new Text(value2);
    }

    public String getVal1() {
        return value1.toString();
    }

    public String getVal2() {
        return value2.toString();
    }

    @Override
    public String toString() {
        return value1.toString()+" "+value2.toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value1.readFields(in);
        value2.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        value1.write(out);
        value2.write(out);
    }
}
