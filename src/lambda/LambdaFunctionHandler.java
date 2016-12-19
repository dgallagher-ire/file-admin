package lambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, Object> {

    @Override
    public Object handleRequest(S3Event input, Context context) {
        context.getLogger().log("Input: " + input);

        // TODO: implement your handler
        return null;
    }
    
    private void copyContentsToFile(final LambdaLogger logger,
			final AmazonS3 s3Client,
			final String bucketName,
			final String key,
			final String contents) throws Exception {
		
		try {
			final String newFileName = key.replace(".dg", ".copy");
			s3Client.putObject(bucketName, newFileName, contents);
			logger.log("File contents copied to: "+newFileName);
		} catch (Exception e) {
			logger.log(e.toString());
			throw e;
		}
	}
    
    private String readS3Contents(final LambdaLogger logger, final String bucketName, final String key)
			throws Exception {

		InputStream in = null;
		try {
			final AmazonS3 s3Client = new AmazonS3Client();
			final GetObjectRequest objectRequest = new GetObjectRequest(bucketName, key);
			final S3Object object = s3Client.getObject(objectRequest);
			in = object.getObjectContent();
			final InputStreamReader inReader = new InputStreamReader(in);
			final BufferedReader reader = new BufferedReader(inReader);
			final StringBuilder sb = new StringBuilder();
			String line;
			while (true) {
				line = reader.readLine();
				if (line == null) {
					break;
				}
				sb.append(line).append("\r\n");
			}
			return sb.toString();
		} catch (Exception e) {
			logger.log(e.toString());
			throw e;
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}


}
