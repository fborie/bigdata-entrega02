package cl.borie.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cloudera on 4/1/15.
 */
public class InvertedIndexReducer extends Reducer<Text, InvertedIndexEntry, Text, InvertedIndexEntry> {

    @Override
    public void reduce(Text key, Iterable<InvertedIndexEntry> values, Context context) throws IOException, InterruptedException {
        long aggregatedCount = 0;
        Map<Long, Long> aggregatedInstances = new HashMap<Long, Long>();
        for (InvertedIndexEntry value : values) {
            aggregatedCount += value.getCount();
            for (Map.Entry<Long, Long> instance : value.getInstances().entrySet()) {
                long perLineCount = instance.getValue();
                if (aggregatedInstances.containsKey(instance.getKey())) {
                    perLineCount += aggregatedInstances.get(instance.getKey());
                }
                aggregatedInstances.put(instance.getKey(), perLineCount);
            }
        }
        context.write(key, new InvertedIndexEntry(aggregatedCount, aggregatedInstances));
    }
}
