package cl.borie.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App extends Configured implements Tool
{
    private static final int SUCCESS = 0;
    private static final int FAILED = 1;

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        //Creating the new Job
        Job job = Job.getInstance();

        // -Setting the basic configuration to run the job-
        // This is optional but will save us time when running the program
        job.setJarByClass(App.class); //<- Give the classname of the tool that will run the job
        job.setJobName(getClass().getSimpleName());

        // Adding the paths for the input and the output
        TextInputFormat.addInputPath(job, new Path(args[0])); // This path is on the 'hdfs'
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Now we have to tell the Job, which will be the output types of the Mapper and the Reducer
        // Mapper Outputs Types (key and value)(intermediate key-value pairs):
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InvertedIndexEntry.class);

        // Reducer Outputs Types (key and value)(final output):
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InvertedIndexEntry.class);

        // Telling the Job which are the Mapper and Reducer Classes of our project:
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // This will wait for the job to finish and return a boolean with the running result
        boolean resultOfRun = job.waitForCompletion(true);
        return resultOfRun ? SUCCESS : FAILED;

    }

    public static void main( String[] args ) throws Exception {
        App test = new App();
        ToolRunner.run(test,args);
    }
}
