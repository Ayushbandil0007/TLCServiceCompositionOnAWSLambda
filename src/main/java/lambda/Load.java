package lambda;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
 * Created by Ayush Bandil on 12/12/2019.
 */

public class Load implements RequestHandler<Request, HashMap<String, Object>> {
    private static String tableName = "sales";
    private static SimpleDateFormat sdfInput = new SimpleDateFormat("MM/dd/yyyy");
    private static SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");

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
        if (con == null){
            logger.log("Could not establish connection with the database");
        }

        logger.log("Processing file:" + filename + " inside " + bucketname);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);
        Response response = new Response();

        //scanning data line by line
        int count = 0;

        try {
            PreparedStatement ps = con.prepareStatement("delete from sales;");
            logger.log("Previous entries have been deleted successfully.");
            ps.execute();

            ps = con.prepareStatement("drop table sales;");
            ps.execute();

            ps = con.prepareStatement(
            "CREATE TABLE sales (\n" +
                    "Region VARCHAR(40), \n" +
                    "Country VARCHAR(40), \n" +
                    "Item_Type VARCHAR(40), \n" +
                    "Sales_Channel VARCHAR(40), \n" +
                    "Order_Priority VARCHAR(40), \n" +
                    "Order_Date DATE, \n" +
                    "Order_ID VARCHAR(40),\n" +
                    "Ship_Date DATE, \n" +
                    "Units_Sold NUMERIC, \n" +
                    "Unit_Price FLOAT(14,4), \n" +
                    "Unit_Cost FLOAT(14,4), \n" +
                    "Total_Revenue FLOAT(14,4), \n" +
                    "Total_Cost FLOAT(14,4), \n" +
                    "Total_Profit FLOAT(14,4), \n" +
                    "Order_Date_Unix_Timestamp VARCHAR(40), \n" +
                    "Ship_Date_Unix_Timestamp VARCHAR(40), \n" +
                    "Order_Processing_Time_Days NUMERIC, \n" +
                    "Gross_Margin FLOAT(10,4), \n" +
                    "Profit_Unit FLOAT(10,4));");
            ps.execute();

            scanner.hasNext();
            scanner.nextLine();
            String line = "";
            Statement statement = null;
            while (scanner.hasNext()) {
                line = scanner.nextLine();
                String[] input = line.split(cvsSplitBy);
                statement.addBatch(getStatement(input));
                if(count%1000 ==0) statement.executeBatch();
                count++;
            }
            statement.executeBatch();
            scanner.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Creating dummy file to trigger Query
        String outputFilename = "dummy.csv";
        StringWriter sw = new StringWriter();
        sw.append("random");
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        s3Client.putObject(bucketname, outputFilename, is, meta);
        logger.log("File " + outputFilename + " Successfully created.");

        //****************END FUNCTION IMPLEMENTATION***************************
        double end = System.currentTimeMillis();
        double timeTaken = (end - start);
        logger.log("TIme taken at server side to process " + count + " rows is " + timeTaken + "ms");
        response.setValue("TIme taken at server side to process " + count + " rows is " + timeTaken + "ms");
        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    static String getStatement(String[] input) {
        String toReturn = "Insert into " + tableName + " (Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit, Order_Date_Unix_Timestamp, Ship_Date_Unix_Timestamp, Order_Processing_Time_Days, Gross_Margin, Profit_Unit) values (";
        toReturn += getString(input[0]) + ", ";
        toReturn += getString(input[1].replaceAll("'", "")) + ", ";
        toReturn += getString(input[2]) + ", ";
        toReturn += getString(input[3]) + ", ";
        toReturn += getString(input[4]) + ", ";
        toReturn += getDate(input[5]) + ", ";
        toReturn += getString(input[6]) + ", ";
        toReturn += getDate(input[7]) + ", ";
        toReturn += input[8] + ", ";
        toReturn += input[9] + ", ";
        toReturn += input[10] + ", ";
        toReturn += input[11] + ", ";
        toReturn += input[12] + ", ";
        toReturn += input[13] + ", ";
        toReturn += getString(input[14]) + ", ";
        toReturn += getString(input[15]) + ", ";
        toReturn += input[16] + ", ";
        toReturn += input[17] + ", ";
        toReturn += input[18];
        toReturn += ");";
        return toReturn;
    }

    private static String getDate(String s) {
        try {
            return "'" + sdfOutput.format(sdfInput.parse(s)) + "'";
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getString(String s) {
        return "'" + s + "'";
    }

    public static Connection getConnection() {
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
            return DriverManager.getConnection(url, username, password);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
