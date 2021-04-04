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
 

   public static void SearchText_old(String text, int pagesize){
      File file = new File("heap" + "." + pagesize);
      
      try {
          InputStream is = new BufferedInputStream(new FileInputStream(file));
          byte[] buffer = new byte[pagesize];
          
          String in;
          int pageCount = 0;
          int count = 0;
          
          System.out.println("Buffer length:" + buffer.length);

          while(is.read(buffer) != -1){
              in = new String(buffer);
              
              System.out.println("in str:" + in);

              System.out.print("Searching in page " + pageCount + "\r");
              
              String[] page = in.split("\r\n");
              for (int i = 0; i < count; i += 184) {
              
            }
              
              System.out.println(page.length);

              for(int j=0; j<page.length-1; j++){
                  String[] token = page[j].split("\t");
                 
                  
                  for(int i=0; i<token.length; i++){
                  
                     System.out.println(token[i]);

                     //split the datainto 2 part
                      String[] data = token[i].split(":",2);
                      
                      //at this point, only search in BN_NAME
                      if(data[0].equals("BN_NAME")){
                          String check1 = text.toLowerCase();
                          String check2 = data[1].toLowerCase();
                          
                          if(check2.contains(check1)){
                              System.out.print("Found '" + text + "' in page " + pageCount + ": ");
                              System.out.print(data[1] + "\r\n");
                              count++;
                          }
                      }
                  }
              }
              pageCount++;
          }
          
          System.out.println();
          System.out.println("Finish loading.");
          System.out.println(count + " result(s) found");
          
      } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
      }
      
  }
  private static String recordField(byte[] record, int offset, int length) {
   String field = new String(record, offset, length, StandardCharsets.ISO_8859_1);

   // Use ASCII NUL as string terminator:
   int pos = field.indexOf('\u0000');
   if (pos != -1) {
       field = field.substring(0, pos);
   }

   return field.trim(); // Trim also spaces for fixed fields.
}
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
byte[] vcc = Arrays.copyOfRange(objs, 0, 25);
String strasddd = new String(vcc);
//System.out.println("text:" + strasddd.trim());

byte[] tmpid = Arrays.copyOfRange(objs, 25, 32);
int tmpidVal= java.nio.ByteBuffer.wrap(tmpid).getInt();
//String strid = String.valueOf(tmpid);
//strid = strid.replaceAll("[\\D]", "");
//int mds = Integer.parseInt(tmpid);
//System.out.println("idValue:" + tmpidVal);

byte[] vdt = Arrays.copyOfRange(objs, 32, 55);
String strdt = new String(vdt);
//System.out.println("text:" + strdt);

byte[] yr = Arrays.copyOfRange(objs, 55, 77);
int inyr = java.nio.ByteBuffer.wrap(yr).getInt();
String stryr = new String(yr);
//System.out.println("year:" + stryr);

byte[] vdt2 = Arrays.copyOfRange(objs, 77, 85);
String strdt2= new String(vdt2);
//System.out.println("month:" + strdt2);

byte[] vmd = Arrays.copyOfRange(objs, 85, 90);
int mDValue = java.nio.ByteBuffer.wrap(vmd).getInt();
//String strmdt = new String(vmd);
//System.out.println("mdate:" + mDateValue);

byte[] mn = Arrays.copyOfRange(objs, 90, 99);
String strmn= new String(mn);
//String strmdt = new String(vmd);
//System.out.println("day:" + strmn);

byte[] tm = Arrays.copyOfRange(objs, 99, 103);
int intm = java.nio.ByteBuffer.wrap(tm).getInt();
String strmdt = new String(tm);
//System.out.println("time:" + intm);


byte[] sid = Arrays.copyOfRange(objs, 103, 107);
int intsid = java.nio.ByteBuffer.wrap(sid).getInt();
String strsid = new String(sid);
//System.out.println("sid:" + intsid);

byte[] sname = Arrays.copyOfRange(objs, 107, 157);
String strsname= new String(sname);
//String strmdt = new String(vmd);
//System.out.println("sname:" + strsname);

byte[] hc = Arrays.copyOfRange(objs, 157, 161);
int inthc = java.nio.ByteBuffer.wrap(hc).getInt();
String strhc = new String(hc);
//System.out.println("hc:" + inthc);
    
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
        // if(recordCount == 1) {
           // recordCount = 1;
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
     // }
     
    }
    objFile.close();
   }
   
   //Validates user input so that page size is a numeric value
   public static int GetPageSize(String[] args) throws Exception {
      
      int position = args.length - 1;
      try {
         int posVal = Integer.parseInt(args[position]);
         return posVal;
      } catch (Exception e) {
         System.out.println("Page size must be a number.");
         System.exit(0);
      }

      return 0;
   }

   //Validates the input text whether it is null. 
   // public static String ValidateString(String[] args) throws Exception {

   //    if (!args[0].isEmpty()) {
   //       String[] arrName = new String[args.length-1];
   //       for (int i = 0; i < args.length - 1; i++) {
   //          arrName[i] = args[i];
   //       }
   //       String fullString = String.join(" ", arrName);
   //       return fullString;
   //    } else {
   //       System.out.println("Please enter text for searching.");
   //       System.exit(0);
   //    }
   //    return null;
   // }

   //Calculate the time taken to search the text in the heap file.
   public static void main(String[] args) {
      long intStartTime = System.nanoTime();
      String text="";
      int intPageSize=0;
      // try {
      //    int intPageSize = GetPageSize(args);
      //    String text = ValidateString(args);
      //    SearchText(text, intPageSize);
      // } catch (Exception e) {
      //    e.printStackTrace();
      // }
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