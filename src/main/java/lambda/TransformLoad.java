package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import saaf.Inspector;
import saaf.Response;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;

import static lambda.Load.getConnection;
import static lambda.Transform.convertToStr;
import static lambda.Transform.performTransformation;


/**
 * Created by Ayush Bandil on 12/12/2019.
 */

public class TransformLoad implements RequestHandler<Request, HashMap<String, Object>> {
    private static int initialLength = 0;
    private static int columnsToAdd = 5;
    private static SimpleDateFormat sdfInput = new SimpleDateFormat("MM/dd/yyyy");
    private static SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd");
    private static String tableName = "sales";
    private static String cvsSplitBy = ",";
    private static int count = 0;


    public HashMap<String, Object> handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        double start = System.currentTimeMillis();
        //Collect inital data.
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        Connection con = getConnection();

        // Perform Transform and Load
        Response response = performTransformLoad(request, context, con, logger);

        double end = System.currentTimeMillis();
        logger.log("TIme taken at server side to process " + count + " rows is " + (end - start) + "ms");
        //Collect final information such as total runtime and cpu deltas.

        // Creating dummy file to trigger Query
        String outputFilename = "dummy.csv";
        StringWriter sw = new StringWriter();
        sw.append("random");
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");
        String bucketname = request.getBucketname();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        s3Client.putObject(bucketname, outputFilename, is, meta);
        logger.log("File " + outputFilename + " Successfully created.");

        inspector.consumeResponse(response);
        inspector.inspectAllDeltas();
        return inspector.finish();
    }

    static Response performTransformLoad(Request request, Context context, Connection con, LambdaLogger logger) {
        String bucketname = request.getBucketname();
        String filename = request.getFilename();
        Response response = new Response();
        String outputFilename = filename.replace(".csv", "") + "_output.csv";
        if (con == null) {
            logger.log("Could not establish connection with the database");
        }

        logger.log("Processing file:" + filename + " inside " + bucketname + " inside TransformLoad");

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                bucketname, filename));    //get object file using source bucket and srcKey name
        InputStream objectData = s3Object.getObjectContent();
        Scanner scanner = new Scanner(objectData);
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement("delete from sales;");
            logger.log("Previous entries have been deleted successfully.");
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //scanning data line by line
        StringWriter sw = new StringWriter();
        Collection<String> orderIdCollection = new HashSet<>();
        scanner.hasNext();
        String line = scanner.nextLine();
        String[] headers = line.split(cvsSplitBy);
        logger.log("updating headers...");
        String[] updatedHeaders = Transform.getUpdatedHeaders(headers);
//        logger.log("Updated headers have " + updatedHeaders.length + " columns");
        sw.append(convertToStr(updatedHeaders, cvsSplitBy));
        count = 0;

        try {
            while (scanner.hasNext()) {
                line = scanner.nextLine();
                String[] input = line.split(cvsSplitBy);
                if (!orderIdCollection.contains(input[6])) {
                    count++;
                    String[] output = performTransformation(input);
                    sw.append(convertToStr(output, cvsSplitBy));
                    ps = con.prepareStatement(Load.getStatement(output));
                    ps.execute();
                    orderIdCollection.add(input[6]);
                } else {
//                logger.log("Order Id " + input[6] + " is already present.");
                }
            }
            scanner.close();
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        byte[] bytes = sw.toString().getBytes(StandardCharsets.UTF_8);
        InputStream is = new ByteArrayInputStream(bytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(bytes.length);
        meta.setContentType("text/plain");

        // Create new file on S3
        logger.log("Creating file " + outputFilename + " with " + count + " lines...");
        s3Client.putObject(bucketname, outputFilename, is, meta);
        logger.log("File " + outputFilename + " Successfully created.");

        response.setValue("Bucket: " + bucketname + " filename:" + filename + " with " + orderIdCollection.size() + " rows processed.");

        //****************END FUNCTION IMPLEMENTATION***************************
        return response;
    }
}