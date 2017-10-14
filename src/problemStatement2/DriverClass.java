
/*
* This is the driver class where specify mappers output type, reducers output type
* file format to be used .
* problemstatement:  MapReduceto calculate the number of cases investigated under FBI
   code 32.
*
 */
 
package problemStatement2;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
 
public class DriverClass {
 
       public static void main(String[] args) throws Exception {
              /*
              *Validate that two arguments were passed from the command line.
              */
              if (args.length != 2) {
                     System.out.printf("Usage:  <input dir> <output dir>\n");
                     System.exit(-1);
              }
             
              /*
              * Instantiate a Configuration object for your job's .
              */
             
              Configuration conf = new Configuration();
              /*
              * Instantiate a Job object for your job's configuration.
              */
             
              Job job = new Job(conf, "Problem_Statement_1");
             
             
 
              /*
              * Specifying the jar file that contains  driver, mapper, and reducer.
              */
              job.setJarByClass(DriverClass.class);
 
              /*
              * Specify the map's output key and value classes.
              */
              job.setMapOutputKeyClass(Text.class);
              job.setMapOutputValueClass(Text.class);
 
              /*
              * Specify the job's output key and value classes.
              */
              job.setOutputKeyClass(Text.class);
              job.setOutputValueClass(IntWritable.class);
              /*
              * Specify the mapper and reducer classes.
              */
              job.setMapperClass(MapperClass.class);
              job.setReducerClass(ReducerClass.class);
             
              job.setInputFormatClass(TextInputFormat.class);
              job.setOutputFormatClass(TextOutputFormat.class);
 
              /*
              * Specify the paths to the input and output data based on the
              * command-line arguments.
              */
             
              FileInputFormat.addInputPath(job, new Path(args[0]));
              FileOutputFormat.setOutputPath(job,new Path(args[1]));
             
              /*
              * Start the MapReduce job and wait for it to finish.
              */
              job.waitForCompletion(true);
       }     
      
      
}