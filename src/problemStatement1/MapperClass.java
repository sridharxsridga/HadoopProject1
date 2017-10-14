package problemStatement1;
 
import java.io.IOException;
 
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jboss.netty.util.internal.StringUtil;
 
public class MapperClass extends Mapper<LongWritable,Text,Text,Text> {
/*
* (non-Javadoc)
* @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
* This function will get byteoffset as key and entire line as value and send fbi code as key and case number as value to reducer
 */
       @Override
       public void map(LongWritable key,Text value , Context context) throws IOException, InterruptedException{
              int fbiCodeIndex=14;  //Index position of FBI code
             
              String line = value.toString();  // Record reader reads the entire line, store that line to string variable
              String[] columns = StringUtil.split(line, ','); // split line with ',' and store it into array
 
                    
              for(int i =0; i <=columns.length-1;i++){  // Read till the length of  record
                     if(columns.length >= fbiCodeIndex){ //Read only if columns in a record is equal or greater than fbiCodeIndex which is 14
                     String case_number = columns[1].trim(); //Store case number
                     String fbi_code = columns[14].trim(); //store fbi code
                     if(!(fbi_code.isEmpty()) &&  !(case_number.isEmpty())){  //If fbi code and case number is blank then ignore else send to reducer through context object
                            context.write(new Text(fbi_code), new Text(case_number));
                     }
              }
              }
       }
 
}