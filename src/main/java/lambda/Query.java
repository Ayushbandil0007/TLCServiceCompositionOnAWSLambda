package lambda;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

import com.amazonaws.services.lambda.runtime.*;
import com.amazonaws.services.s3.model.ObjectMetadata;
import saaf.Inspector;
import saaf.Response;

import static lambda.Load.getConnection;


/**
 * Created by Ayush Bandil on 12/12/2019.
 */
public class Query implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
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
        Connection con = getConnection();
        if (con == null) {
            logger.log("Could not establish connection with the database");
        }

        logger.log("Processing file: ");
        Response response = new Response();

        //scanning data line by line
        int count = 0;

        performQueries(con, logger);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType("text/plain");


        //Create and populate a separate response object for function output. (OPTIONAL)

        //****************END FUNCTION IMPLEMENTATION***************************
        double end = System.currentTimeMillis();
        double timeTaken = (end - start);
        logger.log("TIme taken at server side to process Query is " + timeTaken + "ms");
        response.setValue("TIme taken at server side to process Query is " + timeTaken + "ms");
        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    static void performQueries(Connection con, LambdaLogger logger) {
        try {
            if (con == null) {
                con = getConnection();
            }
            PreparedStatement ps = con.prepareStatement("select version() as version;");
            ResultSet rs = ps.executeQuery();
            ps = con.prepareStatement("Select AVG (Order_Processing_Time_Days) as Avgtime from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Average Processing time for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Avgtime"));
            }


            //Average [Gross Margin] in percent
            ps = con.prepareStatement("select AVG (Gross_Margin) as Avgmargin from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Average Gross Margin for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Avgmargin"));
            }

            //Average [Units Sold]
            ps = con.prepareStatement("select AVG (Units_Sold) as Avgsold from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Average Unit Sold for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Avgsold"));
            }

            //Max [Units Sold]
            ps = con.prepareStatement("select MAX(Units_Sold) as Maxsold from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();

            while (rs.next()) {
                logger.log(" Max Unit Sold for Region: Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Maxsold"));
            }
            //Min [Units Sold]
            ps = con.prepareStatement("select MIN(Units_Sold) as Minsold from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();

            while (rs.next()) {
                logger.log("Min Unit Sold for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Minsold"));
            }


            //Total [Units Sold]
            ps = con.prepareStatement("select SUM(Units_Sold) as Totalsold from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Sum Unit Sold for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Totalsold"));
            }

            //Total [Units Revenue]
            ps = con.prepareStatement("select SUM(Total_revenue) as Totalrev from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Total Revenue for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Totalrev"));
            }

            //Total [Units Profit]
            ps = con.prepareStatement("select SUM(Total_Profit) as Totalprofit from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Total Profit for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Totalprofit"));
            }

            //Number of Orders
            ps = con.prepareStatement("Select Count(ORDER_ID) as Numord from sales where Region = 'Sub-Saharan Africa' and Sales_channel = 'Offline' and Order_Priority ='Low';");
            rs = ps.executeQuery();
            while (rs.next()) {
                logger.log("Total order time for Region: Sub-Saharan Africa, Sales_channel = 'Offline',Order_Priority ='Low' is " + rs.getString("Numord"));
            }
            rs.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
