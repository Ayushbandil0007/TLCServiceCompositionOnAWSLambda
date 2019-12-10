package lambda;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Connection;
import java.util.*;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Load implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
    private static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    private static String tableName = "sales";

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        double start = System.currentTimeMillis();
        String cvsSplitBy = ",";
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        inspector.addTimeStamp("frameworkRuntime");
        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        Connection con = getConnection();

        logger.log("Processing file:" + filename + " inside " + bucketname);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);

        //scanning data line by line
        scanner.hasNext();
        String line = scanner.nextLine();
        int count = 0;
        while (scanner.hasNext()) {
            line = scanner.nextLine();
            String[] input = line.split(cvsSplitBy);
            PreparedStatement ps = null;
            try {
                ps = con.prepareStatement(getStatement(input));
                ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
        scanner.close();
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType("text/plain");


        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
//        response.setValue("Bucket: " + bucketname + " filename:" + filename + " with " + orderIdCollection.size() + " rows processed.");
        inspector.consumeResponse(response);

        //****************END FUNCTION IMPLEMENTATION***************************
        double end = System.currentTimeMillis();
        logger.log("TIme taken at server side to process " + count + " rows is " + (end-start) + "ms");

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private String getStatement(String[] input) {
        String toReturn = "Insert into " + tableName + " (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit, Order_Date_Unix_Timestamp, Ship_Date_Unix_Timestamp, Order_Processing_Time_Days, Gross_Margin, Profit_Unit) values (";
        toReturn+= getString(input[0])  + ", " ;
        toReturn+= input[1] + ", " ;
        toReturn+= input[2] + ", " ;
        toReturn+= input[3] + ", " ;
        toReturn+= input[4] + ", " ;
        toReturn+= input[5] + ", " ;
        toReturn+= input[6] + ", " ;
        toReturn+= input[7] + ", " ;
        toReturn+= input[8] + ", " ;
        toReturn+= input[9] + ", " ;
        toReturn+= input[10] + ", " ;
        toReturn+= input[11] + ", " ;
        toReturn+= input[12] + ", " ;
        toReturn+= input[13] + ", " ;
        toReturn+= input[14] + ", " ;
        toReturn+= input[15] + ", " ;
        toReturn+= input[16] + ", " ;
        toReturn+= input[17] + ", " ;
        toReturn+= input[18];
        toReturn+=");";
        return toReturn;
    }

    private String getString(String s) {
        return "'" + s + "'";
    }

    private Connection getConnection() {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream("db.properties"));
            String url = properties.getProperty("url");
            String username = properties.getProperty("username");
            String password = properties.getProperty("password");
            String driver = properties.getProperty("driver");

            // Manually loading the JDBC Driver is commented out
            // No longer required since JDBC 4
            //Class.forName(driver);
            return DriverManager.getConnection(url,username,password);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String convertToStr(String[] output, String cvsSplitBy) {
        StringBuilder toReturn = new StringBuilder();
        for (String anOutput : output) {
            toReturn.append(anOutput).append(cvsSplitBy);
        }
        toReturn.append("\n");
        return toReturn.toString();
    }

    private static String[] getUpdatedHeaders(String[] headers) {
        initialLength = headers.length;
        String[] toReturn = new String[initialLength + columnsToAdd];
        int i = 0;
        for (i = 0; i < initialLength; i++) {
            toReturn[i] = headers[i];
        }
        toReturn[i++] = "Order Date Unix Timestamp";
        toReturn[i++] = "Ship Date Unix Timestamp";
        toReturn[i++] = "Order Processing Time (Days)";
        toReturn[i++] = "Gross Margin";
        toReturn[i] = "Profit/Unit";
        return toReturn;
    }

    private static String[] performTransformation(String[] input) {
        String[] toReturn = new String[initialLength + columnsToAdd];
        int i = 0;
        for (i = 0; i < initialLength; i++) {
            if (i != 4)
                toReturn[i] = input[i];
            else
                toReturn[i] = input[i].equals("L") ? "Low" : input[i].equals("M") ? "Medium" : input[i].equals("H") ? "High" : "Critical";
        }
        try {
            int orderUnixTime = (int) (sdf.parse(toReturn[5]).getTime() / 1000);
            int shipUnixTime = (int) (sdf.parse(toReturn[7]).getTime() / 1000);
            toReturn[i++] = String.valueOf(orderUnixTime);
            toReturn[i++] = String.valueOf(shipUnixTime);
            toReturn[i++] = String.valueOf((int) ((double) shipUnixTime - (double) orderUnixTime) / 24 / 3600);
            toReturn[i++] = String.valueOf(((double) Math.round(10000 * Double.parseDouble(toReturn[13]) / Double.parseDouble(toReturn[11]))) / 10000);
            toReturn[i] = String.valueOf((double) Math.round(100 * (Double.parseDouble(toReturn[9]) - Double.parseDouble(toReturn[10]))) / 100);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return toReturn;
    }

//    public static void main(String[] args) {
//        String[] output = {"sad", "bad", "mad"};
//        convertToStr(output, ",");
//    }
}
