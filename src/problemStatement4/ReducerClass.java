
/*
*
* This class is to implement the reduce function which will get 
 * "1"  and arrest from map function and calculate the number of arrests  done between October
   2014 and October 2015.
*
 * problemstatement:  MapReduce to calculate the number of arrests done between October
   2014 and October 2015.
*/
 
package problemStatement4;
 
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ReducerClass extends Reducer<Text,Text,Text,IntWritable> {
       /*
       * (non-Javadoc)
       * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
       * This function will get 1 as key and arrest as value
       */
       @Override
       public void reduce(Text key , Iterable<Text> arrest,Context context) throws IOException, InterruptedException
       {
             
 
              int counter=0;  // counter to count the number of arrests
              for(Text value : arrest ){  //iterate through arrests and increment the counter
                     counter++;
              }
             
              context.write(new Text("Number of arrests done between October 2014 and October 2015:"), new IntWritable(counter));//store number of arrests
       }
      
      
}
 