package cl.borie.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

/**
 * Created by cloudera on 3/22/15.
 */
public class TokenizerMapper extends Mapper<LongWritable,Text,Text,InvertedIndexEntry> {

    private Text word;
    private InvertedIndexEntry invertedIndexEntry;


    public TokenizerMapper(){
        word =new Text();
        invertedIndexEntry = new InvertedIndexEntry();
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        while(itr.hasMoreTokens()){
            word.set(itr.nextToken());
            long lineNumber = key.get();
            invertedIndexEntry.setCount(1L);
            HashMap<Long,Long> instances = new HashMap<Long, Long>();
            instances.put(new Long(lineNumber),1L);
            invertedIndexEntry.setInstances(instances);
            context.write(word, invertedIndexEntry);
        }
    }
    
}
