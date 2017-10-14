 
/*
* This class is to implement the map function which will parse the csv data ,  fetch
 * date and  Arrest  from source.
*
 * problemstatement:  MapReduce to calculate the number of arrests done between October
   2014 and October 2015.
 
*/
 
 
 
package problemStatement4;
 
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
 
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
              int dateIndex=2;  //Index position of primaryType
              int arrestIndex=8;  //Index position of arrest
      
              // October 2014 and October 2015 for startDate and endDate
              String startDate= "10/01/2014  00:00:00"; //(format dd/MM/yyyy hh:mm:ss)
              String endDate ="10/31/2015  23:59:59";//(format dd/MM/yyyy hh:mm:ss)
              DateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");
             
              Date stdate = null;
              try {
                     stdate = (Date )format.parse(startDate);
              } catch (ParseException e) {
                    
                     e.printStackTrace();
              }
              Date enddate = null;
              try {
                     enddate = (Date )format.parse(endDate);
              } catch (ParseException e) {
                    
                     e.printStackTrace();
              }
             
             
              String line = value.toString();  // Record reader reads the entire line, store that line to string variable
              String[] columns = StringUtil.split(line, ','); // split line with ',' and store it into array
 
                    
              for(int i =0; i <=columns.length-1;i++){  // Read till the length of  record
                     if(columns.length >= arrestIndex){ //Read only if columns in a record is equal or greater than arrestIndex which is 11
                     String arrestDate = columns[dateIndex].trim(); //clean and  Store date
                     String arrest = columns[arrestIndex].trim(); //clean and store arrest
                           
                    
                     Date date_of_arrest = null;
                     try {
                            date_of_arrest = (Date )format.parse(arrestDate);
                     } catch (ParseException e) {
                           
                            e.printStackTrace();
                     }
                    
                     if((date_of_arrest.after(stdate)) && (date_of_arrest.before(enddate))){
                            context.write(new Text("1"), new Text(arrest));
                     }
                    
      
                           
                    
              }
              }
       }
 
}