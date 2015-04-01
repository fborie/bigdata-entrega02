package cl.borie.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cloudera on 4/1/15.
 */
public class InvertedIndexEntry implements Writable{

    private Long count;
    private Map<Long, Long> instances;

    public InvertedIndexEntry(){
        count = new Long(0);
        instances = new HashMap<Long, Long>();
    }

    public InvertedIndexEntry(Long count, Map<Long, Long> instances){
        this.count = count;
        this.instances = instances;
    }

    public Long getCount(){
        return count;
    }

    public Map<Long,Long> getInstances(){
        return instances;
    }

    public void setCount(Long count){
        this.count = count;
    }

    public void setInstances(Map<Long,Long> instances){
        this.instances = instances;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(count);
        out.writeInt(instances.size());
        for(Map.Entry<Long,Long> instance : instances.entrySet()){
            out.writeLong(instance.getKey());
            out.writeLong(instance.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readLong();
        int instanceCount = in.readInt();
        instances = new HashMap<Long, Long>(instanceCount*2);
        for(int i = 0; i < instanceCount; i++){
            instances.put(in.readLong(),in.readLong());
        }
    }

    @Override
    public String toString(){
        String result = "";
        result += count + ": ";
        for(Map.Entry<Long,Long> instance: instances.entrySet()){
            result += "("+instance.getKey().toString()+","+instance.getValue().toString()+") ";
        }
        return result;
    }
}
