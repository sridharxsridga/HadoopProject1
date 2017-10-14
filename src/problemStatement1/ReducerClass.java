
/*
*
* This class is to implement the reduce function which will get 
 * Case number and FBI code from map function and calculate the number of cases investigated under each
   FBI code
*
 * problemstatement:  MapReduceto calculate number of cases investigated under each
   FBI code
*/
 
 
package problemStatement1;
 
import java.io.IOException;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class ReducerClass extends Reducer<Text,Text,Text,IntWritable> {
       /*
       * (non-Javadoc)
       * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
       * This function will get FBI code as key and case number as value
       */
       @Override
       public void reduce(Text fbiCodekey , Iterable<Text> casesValue,Context context) throws IOException, InterruptedException
       {
              int counter=0;  // counter to count the number of cases for each fbi code
              for(Text value : casesValue ){  //iterate through cases and increment the counter
                     counter++;
              }
             
              context.write(fbiCodekey, new IntWritable(counter));//store fbi code and number of cases investigated by each fbi
      
      
       }
}
 
 