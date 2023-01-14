package org.cdg;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class BlogsUploadS3EventHandler implements RequestHandler<S3Event, String> {
    private final Region REGION = Region.of(System.getenv("region_name"));
    private String DYNAMODB_TABLE_NAME = System.getenv("table_name");
    S3Client s3Client;
    DynamoDbClient ddbClient;

    public BlogsUploadS3EventHandler() {
        s3Client = S3Client.builder().region(REGION).build();
        ddbClient = DynamoDbClient.builder()
                .region(REGION)
                .build();
    }

    public BlogsUploadS3EventHandler(S3Client s3Client, DynamoDbClient amazonDynamoDB) {
        this.s3Client = s3Client;
        this.ddbClient = amazonDynamoDB;

    }

    private void insertIntoDynamoDB(Map<String, String> metadata, String key, LambdaLogger logger) {
        Map<String, AttributeValue> attributesMap = new HashMap<>();

        attributesMap.put("pk", AttributeValue.builder().s("blogs").build());
        attributesMap.put("sk", AttributeValue.builder().s(key).build());
        attributesMap.put("createdDate", AttributeValue.builder().s(Instant.now().toString()).build());
        attributesMap.put("title", AttributeValue.builder().s(metadata.get("title")).build());
        attributesMap.put("description", AttributeValue.builder().s(metadata.get("description")).build());
        attributesMap.put("user", AttributeValue.builder().s((metadata.get("user"))).build());
        PutItemRequest request = PutItemRequest.builder()
                .tableName(DYNAMODB_TABLE_NAME)
                .item(attributesMap)
                .build();
        try {
            PutItemResponse response = ddbClient.putItem(request);
            logger.log(DYNAMODB_TABLE_NAME + " was successfully updated. The request id is " + response.responseMetadata().requestId());

        } catch (DynamoDbException e) {
            logger.log(e.getMessage());
            System.exit(1);
        }

    }

    public String handleRequest(S3Event s3Event, Context context) {
        LambdaLogger logger = context.getLogger();

        s3Event.getRecords().forEach(record -> {
            S3EventNotification.S3BucketEntity bucketEntity = record.getS3().getBucket();
            String key = record.getS3().getObject().getKey();
            logger.log(key);
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketEntity.getName())
                    .key(key)
                    .build();
            HeadObjectResponse headObjectResponse = s3Client.headObject(headObjectRequest);

            Map<String, String> metadata = headObjectResponse.metadata();

            insertIntoDynamoDB(metadata, key, logger);
        });
        return "Uploaded Blog";
    }
}
