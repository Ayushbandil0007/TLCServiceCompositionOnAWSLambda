package lambda;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
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

public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
    private static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        double start = System.currentTimeMillis();
        String cvsSplitBy = ",";
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        String outputFileame = filename.replace(".csv", "") + "_output.csv";

        logger.log("Processing file:" + filename + " inside " + bucketname);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);


        //scanning data line by line
        StringWriter sw = new StringWriter();
        Collection<String> orderIdCollection = new HashSet<>();
        scanner.hasNext();
        String line = scanner.nextLine();
        String[] headers = line.split(cvsSplitBy);
        logger.log("updating headers...");
        String[] updatedHeaders = getUpdatedHeaders(headers);
//        logger.log("Updated headers have " + updatedHeaders.length + " columns");
        sw.append(convertToStr(updatedHeaders, cvsSplitBy));
        int count = 0;
        while (scanner.hasNext()) {
            line = scanner.nextLine();
            String[] input = line.split(cvsSplitBy);
            if (!orderIdCollection.contains(input[6])) {
                count++;
                String[] output = performTransformation(input);
                sw.append(convertToStr(output, cvsSplitBy));
                orderIdCollection.add(input[6]);
            } else {
//                logger.log("Order Id " + input[6] + " is already present.");
            }
        }
        scanner.close();
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        // Create new file on S3
        logger.log("Creating file " + outputFileame + " with " + count + " lines...");
        s3Client.putObject(bucketname, outputFileame, is, meta);
        logger.log("File " + outputFileame + " Successfully created.");

        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: " + bucketname + " filename:" + filename + " with " + orderIdCollection.size() + " rows processed.");

        //****************END FUNCTION IMPLEMENTATION***************************
        double end = System.currentTimeMillis();
        logger.log("TIme taken at server side to process " + count + " rows is " + (end - start) + "ms");
        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    static String convertToStr(String[] output, String cvsSplitBy) {
        StringBuilder toReturn = new StringBuilder();
        for (String anOutput : output) {
            toReturn.append(anOutput).append(cvsSplitBy);
        }
        toReturn.append("\n");
        return toReturn.toString();
    }

    public static String[] getUpdatedHeaders(String[] headers) {
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

    public static String[] performTransformation(String[] input) {
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
}
