
/*
*
* This class is to implement the reduce function which will get 
 * district and primary_type from map function and calculate the number of arrests in theft district wise.
*
 * problemstatement:  MapReduce to  calculate the number of arrests in theft district wise.
*/
 
package problemStatement3;
 
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ReducerClass extends Reducer<Text,Text,Text,IntWritable> {
       /*
       * (non-Javadoc)
       * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
       * This function will get district as key and primary_type as value
       */
       @Override
       public void reduce(Text district , Iterable<Text> primaryType,Context context) throws IOException, InterruptedException
       {
             
 
              int counter=0;  // counter to count the number of arrests
              for(Text value : primaryType ){  //iterate through arrests and increment the counter
                     counter++;
              }
             
              context.write(district, new IntWritable(counter));//store  district and number of arrests
       }
      
      
}