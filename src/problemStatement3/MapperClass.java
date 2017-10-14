/*
* This class is to implement the map function which will parse the csv data ,  fetch
 * Primary type, Arrest and District from source.
*
 * problemstatement:  MapReduce to  calculate the number of arrests in theft district wise.
*/
 
 
 
package problemStatement3;
 
import java.io.IOException;
 
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jboss.netty.util.internal.StringUtil;
 
public class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
/*
* (non-Javadoc)
* @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
* This function will get byteoffset as key and entire line as value and send district  as key and primary_type as value to reducer
 */
       @Override
       public void map(LongWritable key,Text value , Context context) throws IOException, InterruptedException{
              int primaryTypeIndex=5;  //Index position of primaryType
              int arrestIndex=8;  //Index position of arrest
              int districtIndex=11; //Index position of district
             
              String line = value.toString();  // Record reader reads the entire line, store that line to string variable
              String[] columns = StringUtil.split(line, ','); // split line with ',' and store it into array
 
                    
              for(int i =0; i <=columns.length-1;i++){  // Read till the length of  record
                     if(columns.length >= districtIndex){ //Read only if columns in a record is equal or greater than districtIndex which is 11
                     String primary_type = columns[primaryTypeIndex].trim(); //clean and  Store primary type
                     String arrest = columns[arrestIndex].trim(); //clean and store arrest
                     String district = columns[districtIndex].trim();//clean and  store district
                    
                     /*
                     * If primary type is THEFT and ARREST is true then send district and primary_type to reducer
                     */
                     if((primary_type.toUpperCase().equals("THEFT")) && (arrest.toUpperCase().equals("TRUE"))){  
                            context.write(new Text(district), new Text(primary_type));
                     }
                           
                    
              }
              }
       }
 
}