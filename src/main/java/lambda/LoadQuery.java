package lambda;

import java.io.*;
import java.sql.*;
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

import static lambda.Load.getConnection;
import static lambda.Load.getStatement;
import static lambda.Query.performQueries;


/**
 * Created by Ayush Bandil on 12/12/2019.
 */

public class LoadQuery implements RequestHandler<Request, HashMap<String, Object>> {
    private static String tableName = "sales";
    private static SimpleDateFormat sdfInput = new SimpleDateFormat("MM/dd/yyyy");
    private static SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
    private static String cvsSplitBy = ",";

    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        inspector.addTimeStamp("frameworkRuntime");
        Connection con = getConnection();
        if (con == null) {
            logger.log("Could not establish connection with the database");
        }

        // Performing Loading
        Response response = performLoad(request, context, con, logger);

        // Perform Query
        performQueries(con, logger);
        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    private Response performLoad(Request request, Context context, Connection con, LambdaLogger logger) {
        Response response = new Response();
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        logger.log("Processing file:" + filename + " inside " + bucketname);

        double start = System.currentTimeMillis();
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);

        int count = 0;

        try {
            PreparedStatement ps = con.prepareStatement("delete from sales;");
            logger.log("Previous entries have been deleted successfully.");
            ps.execute();
            scanner.hasNext();
            scanner.nextLine();
            String line = "";
            Statement statement = con.createStatement();
            while (scanner.hasNext()) {
                line = scanner.nextLine();
                String[] input = line.split(cvsSplitBy);
                statement.addBatch(getStatement(input));
                count++;
                if (count % 5000 == 0) {
                    statement.executeBatch();
                    logger.log("Processed " + (count / 5000) + "th batch.");
                }
            }
            statement.executeBatch();
            scanner.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType("text/plain");


        //Create and populate a separate response object for function output. (OPTIONAL)

        //****************END FUNCTION IMPLEMENTATION***************************
        double end = System.currentTimeMillis();
        double timeTaken = (end - start);
        logger.log("TIme taken at server side to process Loading " + count + " rows is " + timeTaken + "ms");
        response.setValue("TIme taken at server side to process Loading " + count + " rows is " + timeTaken + "ms");

        return response;
    }
}
