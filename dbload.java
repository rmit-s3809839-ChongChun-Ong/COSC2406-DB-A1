
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
//import com.opencsv.CSVReader;
//Created to store an arraylist of arraylist of records.
//Each page holds an x amount of records.
//This is to reinact how pages work.
// class Page {
//     ArrayList<ArrayList<Row>> lstPage = new ArrayList<ArrayList<Row>>();
//  }
 
 //Row object class to store each line read from the .csv file.
// class Row {
//     int ID;
//     String Date_Time;
//     int Year;
//     String Month;
//     int mDate;
//     String Day;
//     int Time;
//     int SensorID;
//     String SensorName;
//     String SDT_Name;
//     int HourlyCounts;
//  }

public class dbload {

   static final int SDTNameMaxLength = 25;   
   static final int IDMaxLength = 8;
   static final int DTMaxLength = 22;
   static final int yearMaxLength = 4;
   static final int monthMaxLength = 9;
   static final int mDateMaxLength = 4;
   static final int dayMaxLength = 9;
   static final int timeMaxLength = 4;
   static final int sIDMaxLength = 4;
   static final int sNameMaxLength = 50;
   static final int hourlyCountsMaxLength = 4;

    static class page {
       ArrayList<ArrayList<rec>> lstPage = new ArrayList<ArrayList<rec>>();
   }

   static class rec {
    int ID;
    String Date_Time;
    int Year;
    String Month;
    int mDate;
    String Day;
    int Time;
    int SensorID;
    String SensorName;
    String SDT_Name;
    int HourlyCounts;
 }

//InitaliseOutputStream. File created is heap.(pagesize).
public static DataOutputStream GenerateOutputStream(int pageSize) throws FileNotFoundException {
    DataOutputStream oStream = new DataOutputStream(new FileOutputStream("heap." + pageSize));
    return oStream;
 }

 //This method will read data from the .csv file and then use CSVReader to splits into array with values.
 //All strings will be converted to bytes and written to file through DataOutputStream
 //Record size = Total size of all the fields.
 //All records are stored in a page until no more record can fit into the page.
 public static void CreateHeapFile(int pagesize, String datafile){
   File file = new File("heap" + "." + pagesize);
   int pageNum = 1;

   String line = "";  
   String splitBy = ",";  
   try   
   {  
   //parsing a CSV file into BufferedReader class constructor  
   BufferedReader br = new BufferedReader(new FileReader(datafile));  
   DataOutputStream objStream = GenerateOutputStream(pagesize);

   String headerLine = br.readLine();
   page objPage = new page();
   objPage.lstPage.add(new ArrayList<rec>());
  // ArrayList<rec> objPage = new ArrayList<rec>();
   rec objRow = null;
   int intTotalRecordSize= 0;
   int intTotalRowCount = 0;
   int intPageNo = 0;

   while ((line = br.readLine()) != null)   //returns a Boolean value  
   {  
   
   objRow = new rec();

   int SDTLength = 0;
          int IDLength = 0;
          int DTLength = 0;
          int yearLength = 0;
          int monLength = 0;
          int mDtLength = 0;
          int dayLength = 0;
          int timeLength = 0;
          int sIDLength = 0;
          int sNameLength = 0;
          int hcLength = 0;
          int tmpRecordSize = 0;
          int maxLength = 0;
          String strDateTime="";
          String strSensorID="";
          String strSDTNameValue="";

   String[] ArrPC = line.split(splitBy);    // use comma as separator  
     //System.out.println("Array [ID=" + ArrPC[0] + ", DateTime=" + ArrPC[1] + ", Year=" + ArrPC[2] + ", Month=" + ArrPC[3] + ", mDate= " + ArrPC[4] + ", Day= " + ArrPC[5] +"]");  
   
       //Add new field SDT_Name.
       strSDTNameValue = ArrPC[7] + "_" + ArrPC[1];
       byte[] objtmpByte = strSDTNameValue.getBytes("UTF-8");

       byte[] tmpSDTByte = Arrays.copyOf(objtmpByte, SDTNameMaxLength);
       objStream.write(tmpSDTByte);

       objRow.SDT_Name = strSDTNameValue;
       tmpRecordSize += tmpSDTByte.length;
     //  objStream.write(objtmpByte);

     for (int x=0; x < ArrPC.length; x++){
     //   System.out.println("id:" + x );
      //         // byte[] tmpByte = new byte[];
      //          objRow = new Row();
              //  int byteSize = GetByteSize(ArrPC[x]);  
               // byte[] objByte = ArrPC[x].getBytes("UTF-8");
                  
                if (x == 0) {
                  int idValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(idValue);

               //   byte[] objByte = ArrPC[x].getBytes("UTF-8");

               //   if (objByte.length <= IDMaxLength) {
                   byte[] tmpByte = Arrays.copyOf(objByte, IDMaxLength);
                    objStream.write(tmpByte);
                     IDLength = tmpByte.length;
                    // tmpRecordSize += tmpByte.length;
            //     } else {
                    
                     // byte[] objByte = ArrPC[x].getBytes("UTF-8");
            //          objStream.write(objByte);
            //          IDLength = objByte.length;
            //     }
      
                   tmpRecordSize += IDLength;
                  objRow.ID = Integer.parseInt(ArrPC[x]);
                //  System.out.println("id:" + ArrPC[x] );
                }
                else if (x == 1) {
                  byte[] objByte = ArrPC[x].getBytes("UTF-8");

               //   if (objByte.length <= DTMaxLength) {
                     byte[] tmpByte = Arrays.copyOf(objByte, DTMaxLength);
                     objStream.write(tmpByte);
                     DTLength = tmpByte.length;
                //  } else {
                     
                //     objStream.write(objByte);
                //      DTLength = objByte.length;
               //    }
      
                   tmpRecordSize += DTLength;
                  objRow.Date_Time = ArrPC[x];
               //   strDateTime = ArrPC[x];
                }
                else if (x == 2) {
                  int yearValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(yearValue);

             //     if (objByte.length <= yearMaxLength) {
                     byte[] tmpByte = Arrays.copyOf(objByte, DTMaxLength);
                     objStream.write(tmpByte);
                     yearLength = tmpByte.length;
             //     } else {
                   
                    // byte[] objByte = idValue.getBytes("UTF-8");
                   // byte[] objByte = ArrPC[x].getBytes("UTF-8");
              //       objStream.write(objByte);
              //     yearLength = objByte.length;
             //     }
      
                 tmpRecordSize += yearLength;
                objRow.Year = Integer.parseInt(ArrPC[x]);
               }
               else if (x == 3) {
                  byte[] objByte = ArrPC[x].getBytes("UTF-8");

             //   if (objByte.length <= monthMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, monthMaxLength);
                  objStream.write(tmpByte);
                  monLength = tmpByte.length;
            //    } else {
                
                //  objStream.write(objByte);
               //    monLength = objByte.length;

                   System.out.println("Month Length:" + monLength);

           //      }
      
               tmpRecordSize += monLength;
               objRow.Month = ArrPC[x];
      
               }
               else if (x == 4) {
                  int mDateValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(mDateValue);

            //     if (objByte.length <= mDateMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, mDateMaxLength);
                  objStream.write(tmpByte);
                  mDtLength = tmpByte.length;
           //    } else {
                 
          //  System.out.println("mdate value:" + mDateValue);

                 // byte[] objByte = ArrPC[x].getBytes("UTF-8");
          //        objStream.write(objByte);
          //        mDtLength = objByte.length;
          //      }
                
                tmpRecordSize += mDtLength;
                objRow.mDate = Integer.parseInt(ArrPC[x]);
      
               }
               else if (x == 5) {
                  byte[] objByte = ArrPC[x].getBytes("UTF-8");

             //    if (objByte.length <= dayMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, dayMaxLength);
                  objStream.write(tmpByte);
                  dayLength = tmpByte.length;
            //   } else {
                 
            //      objStream.write(objByte);
            //       dayLength = objByte.length;
           //     }
      
                  tmpRecordSize += dayLength;
                 objRow.Day = ArrPC[x];
               }
               else if (x == 6) {
                  int timeValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(timeValue);

             //    if (objByte.length <= timeMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, timeMaxLength);
                  objStream.write(tmpByte);
                  timeLength = tmpByte.length;
          //     } else {
                
                //  byte[] objByte = ArrPC[x].getBytes("UTF-8");
         //          objStream.write(objByte);
         //          timeLength = objByte.length;
          //      }
      
                  tmpRecordSize += timeLength;
                objRow.Time = Integer.parseInt(ArrPC[x]);
               }
               else if (x == 7) {
                  int sensorIDValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(sensorIDValue);

          //       if (objByte.length <= sIDMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, sIDMaxLength);
                  objStream.write(tmpByte);
                  sIDLength = tmpByte.length;
       //       } else {
                
                //  byte[] objByte = ArrPC[x].getBytes("UTF-8");
         //         objStream.write(objByte);
         //          sIDLength = objByte.length;
         //       }
      
               tmpRecordSize += sIDLength;
               objRow.SensorID =  Integer.parseInt(ArrPC[x]);
         //      strSensorID = ArrPC[x];
               }
               else if (x == 8) {
                  byte[] objByte = ArrPC[x].getBytes("UTF-8");

          //       if (objByte.length <= sNameMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, sNameMaxLength);
                  objStream.write(tmpByte);
                  sNameLength = tmpByte.length;
          //     } else {
                 
         //         objStream.write(objByte);
         //          sNameLength = objByte.length;
         //       }
      
                 tmpRecordSize += sNameLength;
                objRow.SensorName = ArrPC[x];
              
               }
               else if (x == 9) {
                  int hourlyCountsValue = Integer.parseInt(ArrPC[x]);
                  byte[] objByte =  intToByteArray(hourlyCountsValue);

            //    if (objByte.length <= hourlyCountsMaxLength) {
                  byte[] tmpByte = Arrays.copyOf(objByte, hourlyCountsMaxLength);
                  objStream.write(tmpByte);
                  hcLength = tmpByte.length;
           //    } else {
                
                //  byte[] objByte = ArrPC[x].getBytes("UTF-8");
           //       objStream.write(objByte);
           //        hcLength = objByte.length;
           //     }
      
                tmpRecordSize += hcLength;
               objRow.HourlyCounts = Integer.parseInt(ArrPC[x]);
             }
      
               
             }

              

            // objPage.add(objRow);
          //  objPage.lstPage.add(new ArrayList<rec>());
          //System.out.println(tmpRecordSize + '~' + intTotalRecordSize + '~' + pagesize);
          System.out.println("total:" + tmpRecordSize + "~" + intTotalRecordSize + "~" + pagesize);

            //  if ((tmpRecordSize + intTotalRecordSize) < pagesize) {
            //             intTotalRecordSize = intTotalRecordSize + tmpRecordSize;
            //             intTotalRowCount++;
            //              objPage.lstPage.add(new ArrayList<rec>());
            //              objPage.lstPage.get(intPageNo).add(objRow);
            //    } else {
            //             intPageNo++;
            //             objPage.lstPage.add(new ArrayList<rec>());
            //             objPage.lstPage.get(intPageNo).add(objRow);
            //              intTotalRecordSize = tmpRecordSize;
            //    }

                 if((tmpRecordSize + intTotalRecordSize) < pagesize){
                    intTotalRowCount++;
                    intTotalRecordSize += tmpRecordSize;
                  //  System.out.println("intpageno:" + intPageNo);

                   objPage.lstPage.get(intPageNo).add(objRow);
                                    
                  }
                else{
                  //             //ensure page page is filled to page size
                  //             byte[] temp = new byte[pagesize];
                  //             System.arraycopy(buffer, 0, temp, 0, buffer.length);
                              
                  //             os.write(temp);
                  //             //pw.println(Arrays.toString(buffer));
                  //             System.out.print("Writing page " + pageNum + ". Previous page size is " + temp.length + "\r");
                              
                  //             //flush buffer
                  //             buffer = new byte[0];
                  //             appendByte(b, buffer);
               
               //   int tmpSize = (pagesize - tmpRecordSize);
               //   byte[] tmpByte = Arrays.copyOf(objByte, tmpSize);
               //   objStream.write(tmpByte);

                    intPageNo++;
                    objPage.lstPage.add(new ArrayList<rec>());
                    objPage.lstPage.get(intPageNo).add(objRow);  
                  //  System.out.println("new intpageno:" + intPageNo);      
                    intTotalRecordSize = tmpRecordSize;
                  }
  
   }  

       //Print out the total no of pages and total no of records
    System.out.println("Total number of pages: " + objPage.lstPage.size());
    int intTotalRows=0;
    for(int i=0; i<objPage.lstPage.size(); i++) {
       for(int j=0; j<objPage.lstPage.get(i).size(); j++) {
         intTotalRows++;
       }
    }
    System.out.println("Total number of records: " + intTotalRows);


   // System.out.println("Size:" + objPage.size());

   // for (int x=0; x < objPage.size(); x++){
   //     System.out.println("ID=" + objPage.get(x).ID + ", DateTime=" + objPage.get(x).Date_Time + ", Year=" + objPage.get(x).Year + ", Month=" + objPage.get(x).Month + ", mDate= " + objPage.get(x).mDate + ", Day= " + objPage.get(x).Day +"]") ;  

   // }

   }   
   catch (IOException e)   
   {  
   e.printStackTrace();  
   }  

   // private static BufferedReader textIn;
   // private static BufferedReader foodFacts;
   //         static int numberOfLines = 0; 
   //         static String [][] foodArray;
   // public static String  aFact;
   // static  int NUM_COL = 7;
   // static int NUM_ROW = 961;
   // String fact;
   // String request = textIn.readLine();
   //       while ( factFile.hasNextLine() && numberOfLines < NUM_ROW){
   //          fact = input.nextLine();
   //          StringTokenizer st2 = new StringTokenizer(fact, ",")    ;
   //          foodArray [numberOfLines++] = fact.split(",");
   //          //facts.add(fact);
   //       // while (st2.hasMoreElements()){
   //       //    for ( int j = 0; j < NUM_COL ; j++) {
   //       //       foodArray [numberOfLines][j]= st2.nextToken(); 
   //       //       System.out.println(foodArray[numberOfLines][i]);
   //       //    }
   //       // }  
   //          numberOfLines++;
   //    }

   // Make a random number to pull a line
   // static Random r = new Random();

   // try {
   //     BufferedReader br = new BufferedReader(new FileReader(datafile));
   //     OutputStream os = new BufferedOutputStream(new FileOutputStream(file));
       
   //     String in;
   //     int page = 0;
   //     byte[] buffer = new byte[0];
       
   //     String[] header = (br.readLine()).split("\t"); //read pass the header
       
   //     while((in = br.readLine()) != null){
   //         String data = new String();
           
   //         //split the line, skip register name
   //         String[] token = in.split("\t");

   //         System.out.println("data:"+header.length);
        
   //         for(int i=0; i<header.length; i++){
   //       //   System.out.println("data:"+token[i]);
   //             try{
   //                 data = data.concat(header[i] + ":" + token[i]);
   //             }
   //             catch(IndexOutOfBoundsException ie){
   //                 data = data.concat(header[i] + ":" + "N/A");
   //             }
               
   //             if(i == header.length-1){
   //                 data = data.concat("\r\n");
   //             }
   //             else{
   //                 data = data.concat("\t");
   //             }
   //         }

           

   //         byte[] b = data.getBytes();
           
   //         //make sure page is enough to contain the data
   //         if((page + b.length) < pagesize){
   //             buffer = appendByte(b, buffer);
   //             page += b.length;
   //         }
   //         else{
   //             //ensure page page is filled to page size
   //             byte[] temp = new byte[pagesize];
   //             System.arraycopy(buffer, 0, temp, 0, buffer.length);
               
   //             os.write(temp);
   //             //pw.println(Arrays.toString(buffer));
   //             System.out.print("Writing page " + pageNum + ". Previous page size is " + temp.length + "\r");
               
   //             //flush buffer
   //             buffer = new byte[0];
   //             appendByte(b, buffer);
               
   //             page = b.length;
   //             pageNum++;
   //         }
           
   //         /*/for testing
   //         if(pageNum == 10){
   //             break;
   //         }*/
   //     }
   //     br.close();
   //     os.close();
       //pw.close();
       
  //     System.out.println();
  //     System.out.println("Finish loading.");
  //     System.out.println("Total page : " + pageNum);
       
  // } catch (FileNotFoundException e) {
       // TODO Auto-generated catch block
 //      e.printStackTrace();
 //  } catch (IOException e) {
 //      // TODO Auto-generated catch block
 //      e.printStackTrace();
 //  }
}

//  public static void CreateHeapFile-old(int pagesize, String datafile){
//    File file = new File("heap" + "." + pagesize);
//    int pageNum = 1;
   
//    try {
//        BufferedReader br = new BufferedReader(new FileReader(datafile));
//        OutputStream os = new BufferedOutputStream(new FileOutputStream(file));
       
//        String in;
//        int page = 0;
//        byte[] buffer = new byte[0];
       
//        String[] header = (br.readLine()).split("\t"); //read pass the header
       
//        while((in = br.readLine()) != null){
//            String data = new String();
           
//            //split the line, skip register name
//            String[] token = in.split("\t");

//          //  System.out.println("data:"+in);
        
//            for(int i=0; i<header.length; i++){
//             System.out.println("data:"+token[i]);
//                try{
//                    data = data.concat(header[i] + ":" + token[i]);
//                }
//                catch(IndexOutOfBoundsException ie){
//                    data = data.concat(header[i] + ":" + "N/A");
//                }
               
//                if(i == header.length-1){
//                    data = data.concat("\r\n");
//                }
//                else{
//                    data = data.concat("\t");
//                }
//            }

           

//            byte[] b = data.getBytes();
           
//            //make sure page is enough to contain the data
//            if((page + b.length) < pagesize){
//                buffer = appendByte(b, buffer);
//                page += b.length;
//            }
//            else{
//                //ensure page page is filled to page size
//                byte[] temp = new byte[pagesize];
//                System.arraycopy(buffer, 0, temp, 0, buffer.length);
               
//                os.write(temp);
//                //pw.println(Arrays.toString(buffer));
//                System.out.print("Writing page " + pageNum + ". Previous page size is " + temp.length + "\r");
               
//                //flush buffer
//                buffer = new byte[0];
//                appendByte(b, buffer);
               
//                page = b.length;
//                pageNum++;
//            }
           
//            /*/for testing
//            if(pageNum == 10){
//                break;
//            }*/
//        }
//        br.close();
//        os.close();
//        //pw.close();
       
//        System.out.println();
//        System.out.println("Finish loading.");
//        System.out.println("Total page : " + pageNum);
       
//    } catch (FileNotFoundException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//    } catch (IOException e) {
//        // TODO Auto-generated catch block
//        e.printStackTrace();
//    }
// }

//  public static void CreateHeapFile(DataOutputStream oStream, File objFile, int intPageSize) throws IOException {
//     CSVReader objReader = new CSVReader(new FileReader(file), '\t');
//     Page objPage = new Page();
//     Row objRow = null;
//     String arrColumns[];
    
//    // int pageSize = intPageSize;
  
      
//     int intTotalRecordSize= 0;
//     int intTotalRowCount = 0;
//     int intPageNo = 0;
//     arrColumns = objReader.readNext();

//     while ((arrColumns = objReader.readNext()) != null) {
//        int SDTLength = 0;
//        int IDLength = 0;
//        int DTLength = 0;
//        int yearLength = 0;
//        int monLength = 0;
//        int mDtLength = 0;
//        int dayLength = 0;
//        int timeLength = 0;
//        int sIDLength = 0;
//        int sNameLength = 0;
//        int hcLength = 0;
//        int tmpRecordSize = 0;
//        int maxLength = 0;
//        String strDateTime="";
//        String strSensorID="";
//        String strSDTNameValue="";
//        //int byteSize = 0;
//        //byte[] objByte = new byte[];

//        //The following convert all the field into bytes before printing to heap
//        for (int x=1; x <=10; x++){
//         // byte[] tmpByte = new byte[];
//          objRow = new Row();
//          int byteSize = GetByteSize(arrColumns[x]);  
//          byte[] objByte = arrColumns[x].getBytes("UTF-8");

//           if (x == 1) {
//             if (byteSize <= IDMaxLength) {
//              byte[] tmpByte = Arrays.copyOf(objByte, IDMaxLength);
//                oStream.write(tmpByte);
//                IDLength = tmpByte.length;
//                tmpRecordSize += tmpByte.length;
//             } else {
//                oStream.write(objByte);
//                IDLength = objByte.length;
//             }

//             tmpRecordSize += IDLength;
//             objRow.ID = arrColumns[x];

//           }
//           else if (x == 2) {
//              if (byteSize <= DTMaxLength) {
//                byte[] tmpByte = Arrays.copyOf(objByte, DTMaxLength);
//                oStream.write(tmpByte);
//                DTLength = tmpByte.length;
//             } else {
//                oStream.write(objByte);
//                DTLength = objByte.length;
//             }

//             tmpRecordSize += DTLength;
//             objRow.Date_Time = arrColumns[x];
//             strDateTime = arrColumns[x];
//           }
//           else if (x == 3) {
//             if (byteSize <= yearMaxLength) {
//                byte[] tmpByte = Arrays.copyOf(objByte, DTMaxLength);
//                oStream.write(tmpByte);
//                yearLength = tmpByte.length;
//             } else {
//                oStream.write(objByte);
//                yearLength = objByte.length;
//             }

//             tmpRecordSize += yearLength;
//             objRow.Year = arrColumns[x];
//          }
//          else if (x == 4) {
//           if (byteSize <= monthMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, monthMaxLength);
//              oStream.write(tmpByte);
//              monLength = tmpByte.length;
//           } else {
//             oStream.write(objByte);
//             monLength = objByte.length;
//           }

//          tmpRecordSize += monLength;
//          objRow.Month = arrColumns[x];

//          }
//          else if (x == 5) {
//            if (byteSize <= mDateMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, mDateMaxLength);
//             oStream.write(tmpByte);
//             mDtLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             mDtLength = objByte.length;
//          }
          
//          tmpRecordSize += mDtLength;
//           objRow.mDate = arrColumns[x];

//          }
//          else if (x == 6) {
//            if (byteSize <= dayMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, dayMaxLength);
//             oStream.write(tmpByte);
//             dayLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             dayLength = objByte.length;
//          }

//            tmpRecordSize += dayLength;
//            objRow.Day = arrColumns[x];
//          }
//          else if (x == 7) {
//            if (byteSize <= timeMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, timeMaxLength);
//             oStream.write(tmpByte);
//             timeLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             timeLength = objByte.length;
//          }

//            tmpRecordSize += timeLength;
//            objRow.Time = arrColumns[x];
//          }
//          else if (x == 8) {
//            if (byteSize <= sIDMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, sIDMaxLength);
//             oStream.write(tmpByte);
//             sIDLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             sIDLength = objByte.length;
//          }

//          tmpRecordSize += sIDLength;
//          objRow.SensorID = arrColumns[x];
//          strSensorID = arrColumns[x];
//          }
//          else if (x == 9) {
//            if (byteSize <= sNameMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, sNameMaxLength);
//             oStream.write(tmpByte);
//             sNameLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             sNameLength = objByte.length;
//           }

//           tmpRecordSize += sNameLength;
//           objRow.SensorName = arrColumns[x];
        
//          }
//          else if (x == 10) {
//           if (byteSize <= hourlyCountsMaxLength) {
//             byte[] tmpByte = Arrays.copyOf(objByte, hourlyCountsMaxLength);
//             oStream.write(tmpByte);
//             hcLength = tmpByte.length;
//          } else {
//             oStream.write(objByte);
//             hcLength = objByte.length;
//          }

//          tmpRecordSize += hcLength;
//          objRow.HourlyCounts = arrColumns[x];
//        }

         
//        }

//        //Add new field SDT_Name.
//        strSDTNameValue = strSensorID  + strDateTime;
//        byte[] objByte = strSDTNameValue.getBytes("UTF-8");
//        objRow.SDT_Name = strSDTNameValue;
//        tmpRecordSize += sIDLength + DTLength;
//        oStream.write(objByte);

//      // tmpRecordSize = nB + sB + dorB + docB + rdB + fsnB + psorB + abnB;

//        //This checks that record fits into the page and if not
//        //create a new page.
//        if ((tmpRecordSize + intTotalRecordSize) < intPageSize) {
//          intTotalRecordSize = intTotalRecordSize + tmpRecordSize;
//          intTotalRowCount++;
//           objPage.lstPage.add(new ArrayList<Row>());
//           objPage.lstPage.get(intPageNo).add(objRow);
//        } else {
//          intPageNo++;
//          objPage.lstPage.add(new ArrayList<Row>());
//          objPage.lstPage.get(intPageNo).add(objRow);
//           intTotalRecordSize = tmpRecordSize;
//        }
//     }
    
//     //Print out the total no of pages and total no of records
//     System.out.println("Total number of pages: " + objPage.lstPage.size());
//     int intTotalRows=0;
//     for(int i=0; i<objPage.lstPage.size(); i++) {
//        for(int j=0; j<objPage.lstPage.get(i).size(); j++) {
//          intTotalRows++;
//        }
//     }
//     System.out.println("Total number of records: " + intTotalRows);
//  }

private static byte[] intToByteArray(final int i ) throws IOException {      
   byte[] bytes = java.nio.ByteBuffer.allocate(4).putInt(i).array();

   return bytes;
}

public static byte[] intToByteArray_old(int value) {
   return new byte[] {
           (byte)(value >> 24),
           (byte)(value >> 16),
           (byte)(value >> 8),
           (byte)value};
}

 //Called to convert the strings to bytes
 public static int GetByteSize(String string) throws UnsupportedEncodingException {
    final byte[] bt = string.getBytes("UTF-8");
    int btSize = bt.length;
    return btSize;
 }

 //Check page size entered is valud and if it is then get the page size.
 public static int GetPageSize(String[] args) throws Exception {

    int position = args.length - 2;
    try {
       int pgs = Integer.parseInt(args[position]);
       return pgs;
    } catch (Exception e) {
       System.out.println("Page size provided must be a number.");
       System.exit(0);
    }
    return 0;
 }

 //Check if input file is a valid file and if it is then return as a file.
 public static File GetFile(String[] args) throws Exception {

    File objFile = new File(args[2]);
    if (!args[0].equals("-p") || args[1].isEmpty() || !objFile.exists()) {
       System.out.print("Invalid arguments or file does not exist. Please check and try again.");
       System.exit(0);
    }
    return objFile;
 }

 public static byte[] appendByte(byte[] data, byte[] destination){
   byte[] temp = new byte[destination.length + data.length];
   
   //copy all data into temp
   System.arraycopy(destination, 0, temp, 0, destination.length);
   System.arraycopy(data, 0, temp, destination.length, data.length);
   
   destination = temp;
   
   return destination;
}

 //Main method
 //Calculates the time taken to create heap file in ms.
 public static void main(String[] args) {
    long intStartTime = System.nanoTime();
    try {
      // File objFile = GetFile(args);
     //  int objPageSize = GetPageSize(args);
    //   DataOutputStream objOutputStream = GenerateOutputStream(objPageSize);
     //  CreateHeapFile(objOutputStream, objFile, objPageSize);

        int pagesize = 0;
        String datafile = null;
        try{
            pagesize = Integer.parseInt(args[1]);
            datafile = args[2];    
        }
        catch(Exception e){
            System.out.println("Invalid arguments. Please try again");
            System.exit(0);
        }

       
      //  long startTime, endTime, duration;
        
      //  startTime = System.currentTimeMillis();
     //   startTime = System.nanoTime();  
        CreateHeapFile(pagesize,datafile);
       // endTime = System.nanoTime();
       // duration = (endTime - startTime);
        long intEndTime = System.nanoTime();
        long intTotalTime = (intEndTime - intStartTime) / 1000000;
        System.out.println("Heap file creation completed in: " + intTotalTime + "ms");

     //   System.out.println("Time taken for creating heapfile page size " + pagesize + ": " + duration + "s");


    } catch (Exception e) {
     //  e.printStackTrace();
      System.err.println(e.getMessage());
    }
   
 }

 
}