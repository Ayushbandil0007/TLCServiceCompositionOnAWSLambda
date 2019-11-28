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
 * uwt.lambda_test::handleRequest
 *
 * @author Wes Lloyd
 * @author Robert Cordingly
 */
public class Transform implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
    private static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");

    public HashMap<String, Object> handleRequest(Request request, Context context) {

        String cvsSplitBy = ",";
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        String bucketname = request.getBucketname();
        String filename = request.getFilename();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);


        //scanning data line by line
        StringWriter sw = new StringWriter();
        List<String[]> outputList = new ArrayList<>();
        Collection<String> orderIdCollection = new HashSet<>();
        scanner.hasNext();
        String line = scanner.nextLine();
        String[] headers = line.split(cvsSplitBy);
        String[] updatedDHeaders = getUpdatedHeaders(headers);
        outputList.add(updatedDHeaders);
        while (scanner.hasNext()) {
            String[] input = line.split(cvsSplitBy);
            if (!orderIdCollection.contains(input[6])) {
                String[] output = performTransformation(input);
                outputList.add(output);
                sw.append(convertToStr(output));
                orderIdCollection.add(input[6]);
            }
        }
        scanner.close();
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        // Create new file on S3
        s3Client.putObject(bucketname, filename, is, meta);

        LambdaLogger logger = context.getLogger();
        logger.log("Transform  bucketname:" + bucketname + " filename:" + filename);


        //Create and populate a separate response object for function output. (OPTIONAL)
        Response response = new Response();
        response.setValue("Bucket: " + bucketname + " filename:" + filename + " with " + orderIdCollection.size() + " rows processed.");
        inspector.consumeResponse(response);

        //****************END FUNCTION IMPLEMENTATION***************************

        //Collect final information such as total runtime and cpu deltas.
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private String convertToStr(String[] output) {
        StringBuilder toReturn = new StringBuilder();
        for (String anOutput : output) {
            toReturn.append(anOutput);
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

}
