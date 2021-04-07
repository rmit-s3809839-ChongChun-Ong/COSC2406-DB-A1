import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.io.*;
public class dbquery {

   //This method search the input text against the heap file created by dbLoad program 
   //If a match is found, it will print all values including the P=page and record number
    
   static final int SDTNameMaxLength = 80;   
   static final int IDMaxLength = 8;
   static final int DTMaxLength = 11;
   static final int yearMaxLength = 4;
   static final int monthMaxLength = 9;
   static final int mDateMaxLength = 2;
   static final int dayMaxLength = 9;
   static final int timeMaxLength = 2;
   static final int sIDMaxLength = 2;
   static final int sNameMaxLength = 50;
   static final int hourlyCountsMaxLength = 4;
 
   //Search the text from the input arguments
   //Field bytes will be retrieved based on range position determined by the field length
   public static void SearchText(String text, int pageSize) throws IOException {
      RandomAccessFile objFile = new RandomAccessFile("heap." + pageSize, "r");

      Path path = Paths.get("heap." + pageSize);
      byte[] bytes = Files.readAllBytes(path);
      int count = bytes.length;

      int pageNum = 1;
      int recordCount = 1;
      int tmpTotalRecordCountPerPage = pageSize/161;  

         
objFile.seek(161);
byte[] objs = new byte[161];
objFile.read(objs);
    
      for (int i = 0; i < count; i += 161) {
        
         objFile.seek(i);
        byte[] objRecordByte = new byte[161];
         objFile.read(objRecordByte);
       
         byte[] SDT_Name = Arrays.copyOfRange(objRecordByte, 0, 25);
           byte[] id = Arrays.copyOfRange(objRecordByte, 25, 32);
           byte[] date_time = Arrays.copyOfRange(objRecordByte, 32, 55);
           byte[] year = Arrays.copyOfRange(objRecordByte, 55, 77);
          byte[] month = Arrays.copyOfRange(objRecordByte, 77, 85);
          byte[] mDate = Arrays.copyOfRange(objRecordByte, 86, 90);
          byte[] day = Arrays.copyOfRange(objRecordByte, 90, 99);
          byte[] time = Arrays.copyOfRange(objRecordByte, 99, 103);
          byte[] sensorID = Arrays.copyOfRange(objRecordByte, 103, 107);
          byte[] sensorName = Arrays.copyOfRange(objRecordByte, 107, 157);
          byte[] hourlycounts = Arrays.copyOfRange(objRecordByte, 157, 161);
       

         int idValue = java.nio.ByteBuffer.wrap(id).getInt();
         int yearValue = java.nio.ByteBuffer.wrap(year).getInt();
        int mDateValue = java.nio.ByteBuffer.wrap(mDate).getInt();
        int timeValue = java.nio.ByteBuffer.wrap(time).getInt();
        int sensorIDValue = java.nio.ByteBuffer.wrap(sensorID).getInt();
       int hourlycountsValue = java.nio.ByteBuffer.wrap(hourlycounts).getInt();
    
       String strSDTname = new String(SDT_Name);
    
         if(strSDTname.toLowerCase().contains(text.toLowerCase())) {
            System.out.println("Record: " + recordCount);
            System.out.println("Page: " + pageNum);
            System.out.println("SDT_Name: " + strSDTname);
            System.out.println("ID: " + idValue);
            System.out.println("Date_Time: " + new String(date_time));
            System.out.println("Year: " + yearValue);
             System.out.println("Month: " + new String(month));
             System.out.println("mDate: " + mDateValue);
            System.out.println("Day: " + new String(day));
            System.out.println("Time: " + timeValue);
            System.out.println("Sensor_ID: " + sensorIDValue);
            System.out.println("SensorName: " + new String(sensorName));
            System.out.println("HourlyCounts: " + hourlycountsValue);
            System.out.println();
         }
         else {
            recordCount++;
            if(recordCount == tmpTotalRecordCountPerPage) {
               pageNum++;
               recordCount=0;
            }
         }
        
    }
    objFile.close();
   }
   
   //Calculate the time taken to search the text in the heap file.
   public static void main(String[] args) {
      long intStartTime = System.nanoTime();
      String text="";
      int intPageSize=0;
     
    try{
      try{
         text = args[0];
         intPageSize = Integer.parseInt(args[1]);
     }
     catch(Exception e){
         e.printStackTrace();
         System.out.println("Invalid arguments. Please try again");
         System.exit(0);
     }
      
     SearchText(text, intPageSize); 
      long intEndTime = System.nanoTime();
      long intTotalTime = (intEndTime - intStartTime) / 1000000;
      System.out.println("Search completed in: " + intTotalTime + "ms");
   }
   catch(Exception e){
      e.printStackTrace();
   
  }
}

}