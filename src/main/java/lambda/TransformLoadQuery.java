package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import saaf.Response;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import static lambda.Load.getConnection;
import static lambda.Query.performQueries;
import static lambda.TransformLoad.performTransformLoad;


/**
 * Created by Ayush Bandil on 12/12/2019.
 */

public class TransformLoadQuery implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
    private static SimpleDateFormat sdfInput = new SimpleDateFormat("MM/dd/yyyy");
    private static SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
    private static String tableName = "sales";

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        double start = System.currentTimeMillis();
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        Connection con = getConnection();
        if (con == null) {
            logger.log("Could not establish connection with the database");
        }
        Response response = performTransformLoad(request, context, con, logger);
        performQueries(con, logger);

        double end = System.currentTimeMillis();
        logger.log("TIme taken at server side to process complete TLQ is " + (end - start) + "ms");
        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
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
            int orderUnixTime = (int) (sdfInput.parse(toReturn[5]).getTime() / 1000);
            int shipUnixTime = (int) (sdfInput.parse(toReturn[7]).getTime() / 1000);
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
}