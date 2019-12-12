//package lambda;
//
//import com.opencsv.CSVWriter;
//
//import java.io.*;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
///**
// * Created by Ayush Bandil on 16/11/2019.
// */
//public class LocalTesting {
//    private static int initialLength = 0;
//    private static int columnsToAdd = 5;
//    private static SimpleDateFormat sdfInput = new SimpleDateFormat("MM/dd/yyyy");
//    private static SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
//
//    private static String tableName = "sales";
//
//    public static void main(String[] args) {
//        String csvFile = "C:\\Users\\Ayush Bandil\\Downloads\\Q1\\TCSS 562  Software Engineering For Cloud Computing\\project\\100-Sales-Records\\100_Sales_Records_output.csv";
//
//        String line = "";
//        String cvsSplitBy = ",";
//        File file = new File("C:\\Users\\Ayush Bandil\\Downloads\\Q1\\TCSS 562  Software Engineering For Cloud Computing\\project\\100-Sales-Records\\100 Sales RecordsProcessed.csv");
//        try {
//            BufferedReader br = new BufferedReader(new FileReader(csvFile));
//            FileWriter outputfile = new FileWriter(file);
//            CSVWriter writer = new CSVWriter(outputfile);
//            List<String[]> outputList = new ArrayList<>();
//            Collection<String> orderIdCollection = new HashSet<>();
//            line = br.readLine();
//            String[] headers = line.split(cvsSplitBy);
//            String[] updatedDHeaders = getUpdatedHeaders(headers);
//            outputList.add(updatedDHeaders);
//            while ((line = br.readLine()) != null) {
//                String[] input = line.split(cvsSplitBy);
//                String statement = getStatement(input);
//                if (!orderIdCollection.contains(input[6])) {
//                    String[] output = performTransformation(input);
//                    outputList.add(output);
//                    orderIdCollection.add(input[6]);
//                }
//            }
//            writer.writeAll(outputList);
//            writer.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static String[] getUpdatedHeaders(String[] headers) {
//        initialLength = headers.length;
//        String[] toReturn = new String[initialLength + columnsToAdd];
//        int i = 0;
//        for (i = 0; i < initialLength; i++) {
//            toReturn[i] = headers[i];
//        }
//        toReturn[i++] = "Order Date Unix Timestamp";
//        toReturn[i++] = "Ship Date Unix Timestamp";
//        toReturn[i++] = "Order Processing Time (Days)";
//        toReturn[i++] = "Gross Margin";
//        toReturn[i++] = "Profit/Unit";
//        return toReturn;
//    }
//
//    private static String[] performTransformation(String[] input) {
//        String[] toReturn = new String[initialLength + columnsToAdd];
//        int i = 0;
//        for (i = 0; i < initialLength; i++) {
//            if (i != 4)
//                toReturn[i] = input[i];
//            else
//                toReturn[i] = input[i].equals("L") ? "Low" : input[i].equals("M") ? "Medium" : input[i].equals("H") ? "High" : "Critical";
//        }
//        try {
//            int orderUnixTime = (int) (sdfInput.parse(toReturn[5]).getTime() / 1000);
//            int shipUnixTime = (int) (sdfInput.parse(toReturn[7]).getTime() / 1000);
//            toReturn[i++] = String.valueOf(orderUnixTime);
//            toReturn[i++] = String.valueOf(shipUnixTime);
//            toReturn[i++] = String.valueOf((int) ((double) shipUnixTime - (double) orderUnixTime) / 24 / 3600);
//            toReturn[i++] = String.valueOf(((double) Math.round(10000 * Double.parseDouble(toReturn[13]) / Double.parseDouble(toReturn[11]))) / 10000);
//            toReturn[i] = String.valueOf((double) Math.round(100 * (Double.parseDouble(toReturn[9]) - Double.parseDouble(toReturn[10]))) / 100);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return toReturn;
//    }
//
//    private static String getStatement(String[] input) {
//        String toReturn = "Insert into " + tableName + " (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit, Order_Date_Unix_Timestamp, Ship_Date_Unix_Timestamp, Order_Processing_Time_Days, Gross_Margin, Profit_Unit) values (";
//        toReturn += getString(input[0]) + ", ";
//        toReturn += getString(input[1]) + ", ";
//        toReturn += getString(input[2]) + ", ";
//        toReturn += getString(input[3]) + ", ";
//        toReturn += getString(input[4]) + ", ";
//        toReturn += getDate(input[5]) + ", ";
//        toReturn += getString(input[6]) + ", ";
//        toReturn += getDate(input[7]) + ", ";
//        toReturn += input[8] + ", ";
//        toReturn += input[9] + ", ";
//        toReturn += input[10] + ", ";
//        toReturn += input[11] + ", ";
//        toReturn += input[12] + ", ";
//        toReturn += input[13] + ", ";
//        toReturn += getString(input[14]) + ", ";
//        toReturn += getString(input[15]) + ", ";
//        toReturn += input[16] + ", ";
//        toReturn += input[17] + ", ";
//        toReturn += input[18];
//        toReturn += ");";
//        return toReturn;
//    }
//
//    private static String getDate(String s) {
//        try {
//            return "'" + sdfOutput.format(sdfInput.parse(s)) + "'";
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    private static String getString(String s) {
//        return "'" + s + "'";
//    }
//
//}
