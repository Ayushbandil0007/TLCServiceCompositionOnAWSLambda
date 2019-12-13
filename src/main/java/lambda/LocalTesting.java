//package lambda;
//
//import com.opencsv.CSVWriter;
//
//import java.io.*;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//import static lambda.Load.getStatement;
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
//                if (!orderIdCollection.contains(input[6])) {
//                    String[] output = performTransformation(input);
//                    outputList.add(output);
//                    String statement = getStatement(input);
//
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
//}
