package lambda;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;

import lambda.file.Record;
import lambda.file.Records;

public class LambdaFunctionHandler implements RequestHandler<RequestClass, ResponseClass> {

	@Override
	public ResponseClass handleRequest(RequestClass input, Context context) {
		final LambdaLogger logger = context.getLogger();
		try {
			final ObjectMapper mapper = new ObjectMapper();
			logger.log(mapper.writeValueAsString(input));
			final AmazonS3 s3Client = new AmazonS3Client(); 
			final Records records = getRecords(s3Client);
			if("ADD".equals(input.getAction())){
				logger.log("Add record");
				records.addRecords(buildNewRecord(input));
			}
			logger.log(mapper.writeValueAsString(records));
			addFile(logger, s3Client, "dgallagher-bucket", "loader-data.json", mapper.writeValueAsString(records));
			return new ResponseClass("success");
		} catch (Exception e) {
			logger.log(e.toString());
			return new ResponseClass("failed");
		}
	}
	
	private static Record buildNewRecord(final RequestClass input) {
		final Record newRecord = new Record();
		newRecord.setBucket(input.getBucket());
		newRecord.setLive(input.getLive());
		newRecord.setFiles(input.getFiles());
		newRecord.setBatch(input.getBatch());
		newRecord.setRedShift(input.getRedShift());
		return newRecord;
	}

	private static void addFile(final LambdaLogger logger, final AmazonS3 s3Client, final String bucketName, final String key,
			final String contents) throws Exception {

		try {
			s3Client.putObject(bucketName, key, contents);
			logger.log("File written to: " + key);
		} catch (Exception e) {
			logger.log(e.toString());
			throw e;
		}
	}

	private static Records getRecords(final AmazonS3 s3Client) throws Exception {
		final StringBuilder sb = new StringBuilder();
		final String loaderJson = getFileContents(s3Client, "dgallagher-bucket", "loader-data.json");
		if (loaderJson == null || loaderJson.equals("")) {
			return new Records();
		}
		final ObjectMapper mapper = new ObjectMapper();
		final Records records = mapper.readValue(loaderJson, Records.class);
		return records;
	}

	private static String getFileContents(final AmazonS3 s3Client, final String bucketName, final String fileName)
			throws Exception {

		InputStream in = null;
		try {
			final S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
			in = s3object.getObjectContent();
			final BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line = null;
			final StringBuilder sb = new StringBuilder();
			while (true) {
				line = reader.readLine();
				if (line == null) {
					break;
				}
				sb.append(line).append("\r\n");
			}
			return sb.toString();
		} catch (Exception e) {
			throw e;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
				}
			}
		}
	}

}
