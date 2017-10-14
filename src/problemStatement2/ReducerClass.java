
 
/*
*
* This class is to implement the reduce function which will get 
 * Case number and FBI code from map function and calculate the number of cases investigated under FBI
   code 32.
*
 * problemstatement:  MapReduceto calculate the number of cases investigated under FBI
   code 32.
*/
 
package problemStatement2;
 
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
             
       if(fbiCodekey.equals(new Text("32"))){ //If FBI code is equal to 32
              int counter=0;  // counter to count the number of cases
              for(Text value : casesValue ){  //iterate through cases and increment the counter
                     counter++;
              }
             
              context.write(fbiCodekey, new IntWritable(counter));//store fbi code and number of cases investigated by each fbi
       }
      
       }
}