package pagerank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.AttributeUpdate;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;

public class DynamoDBWrapper {

	
	 /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/home/yupenglu/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonDynamoDBClient dynamoDB;
    private String tableName;
    private Table table;
    private final String keyName = "key";

    public DynamoDBWrapper(String tableName) {
		this(tableName, null);
	}
    
    public DynamoDBWrapper(String tableName, String endPoint) {
    	this(tableName, 100, 100, endPoint);
    }
    
    public DynamoDBWrapper(String tableName, int writeThroughput, int readThroughput, String endPoint) {
    	this.tableName = tableName;
    	try {
			init(readThroughput, writeThroughput, endPoint);
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public void setEndpoint(String endPoint) {
    	dynamoDB.setEndpoint(endPoint);
    }
    
    
    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.ProfilesConfigFile
     * @see com.amazonaws.ClientConfiguration
     */
    private void init(long writeThroughput, long readThroughput, String endPoint) throws Exception {
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (/home/yupenglu/.aws/credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/yupenglu/.aws/credentials), and is in valid format.",
                    e);
        }
        dynamoDB = new AmazonDynamoDBClient(credentials);
        if (endPoint != null) {
        	dynamoDB.setEndpoint(endPoint);
        } else {
        	Region usEast = Region.getRegion(Regions.US_EAST_1);
        	dynamoDB.setRegion(usEast);
        }
        
        // Create table if it does not exist yet
        if (Tables.doesTableExist(dynamoDB, tableName)) {
        	System.out.println("Table " + tableName + " is already ACTIVE");
        } else {
        	// Create a table with a primary hash key named 'name', which holds a string
        	CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
        			.withKeySchema(new KeySchemaElement().withAttributeName("key").withKeyType(KeyType.HASH))
        			.withAttributeDefinitions(new AttributeDefinition().withAttributeName("key").withAttributeType(ScalarAttributeType.S))
        			.withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(readThroughput).withWriteCapacityUnits(writeThroughput));
        	TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
        	System.out.println("Created Table: " + createdTableDescription);

        	// Wait for it to become active
        	System.out.println("Waiting for " + tableName + " to become ACTIVE...");
        	Tables.awaitTableToBecomeActive(dynamoDB, tableName);
        }
        
        //Describe our new table
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
        System.out.println("Table Description: " + tableDescription);
        
        table = new DynamoDB(dynamoDB).getTable(tableName);
    }
    
    public Set<String> getSet(String key, String attName) {
    	
//    	HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
//    	Condition condition = new Condition()
//    	.withComparisonOperator(ComparisonOperator.GT.toString())
//    	.withAttributeValueList(new AttributeValue().withN("1985"));
//    	scanFilter.put("year", condition);
//    	ScanRequest scanRequest = new ScanRequest(tableName);
//    	ScanResult scanResult = dynamoDB.scan(scanRequest);
//    	System.out.println("Result: " + scanResult);

    	Item item = table.getItem(keyName, key);
    	return item.getStringSet(attName);
    }
    
    public void put(String key, String attName, String... values) {
//    	AttributeUpdate update = new AttributeUpdate(attName);
//    	update.addElements(new AttributeValue().withSS(value));
    	
    	Map<String, String> expressionAttributeNames = new HashMap<String, String>();
    	expressionAttributeNames.put("#A", attName);

    	Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
    	expressionAttributeValues.put(":val1",
    	    new HashSet<String>(Arrays.asList(values)));

    	UpdateItemOutcome outcome =  table.updateItem(
    		    keyName,          // key attribute name
    		    key,           // key attribute value
    		    "add #A :val1", // UpdateExpression
    		    expressionAttributeNames,
    		    expressionAttributeValues);
    	
//    	Set<String> set = new HashSet<String>();
//    	set.add(value);
//    	update.addElements(set);
//    	table.updateItem(key, update);
    }

//    public static void main(String[] args) throws Exception {
//        init();
//
//        try {
//            String tableName = "TestTest";
//
//            // Create table if it does not exist yet
//            if (Tables.doesTableExist(dynamoDB, tableName)) {
//                System.out.println("Table " + tableName + " is already ACTIVE");
//            } else {
//                // Create a table with a primary hash key named 'name', which holds a string
//                CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
//                    .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
//                    .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
//                    .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
//                    TableDescription createdTableDescription = dynamoDB.createTable(createTableRequest).getTableDescription();
//                System.out.println("Created Table: " + createdTableDescription);
//
//                // Wait for it to become active
//                System.out.println("Waiting for " + tableName + " to become ACTIVE...");
//                Tables.awaitTableToBecomeActive(dynamoDB, tableName);
//            }
//
//            // Describe our new table
//            DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
//            TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
//            System.out.println("Table Description: " + tableDescription);
//
//            // Add an item
//            Map<String, AttributeValue> item = newItem("Bill & Ted's Excellent Adventure", 1989, "****", "James", "Sara");
//            PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
//            PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
//            System.out.println("Result: " + putItemResult);
//
//            // Add another item
//            item = newItem("Airplane", 1980, "*****", "James", "Billy Bob");
//            putItemRequest = new PutItemRequest(tableName, item);
//            putItemResult = dynamoDB.putItem(putItemRequest);
//            System.out.println("Result: " + putItemResult);
//
//            // Scan items for movies with a year attribute greater than 1985
//            HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
//            Condition condition = new Condition()
//                .withComparisonOperator(ComparisonOperator.GT.toString())
//                .withAttributeValueList(new AttributeValue().withN("1985"));
//            scanFilter.put("year", condition);
//            ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
//            ScanResult scanResult = dynamoDB.scan(scanRequest);
//            System.out.println("Result: " + scanResult);
//
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught an AmazonServiceException, which means your request made it "
//                    + "to AWS, but was rejected with an error response for some reason.");
//            System.out.println("Error Message:    " + ase.getMessage());
//            System.out.println("HTTP Status Code: " + ase.getStatusCode());
//            System.out.println("AWS Error Code:   " + ase.getErrorCode());
//            System.out.println("Error Type:       " + ase.getErrorType());
//            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
//            System.out.println("Caught an AmazonClientException, which means the client encountered "
//                    + "a serious internal problem while trying to communicate with AWS, "
//                    + "such as not being able to access the network.");
//            System.out.println("Error Message: " + ace.getMessage());
//        }
//    }

//    private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
//        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
//        item.put("name", new AttributeValue(name));
//        item.put("year", new AttributeValue().withN(Integer.toString(year)));
//        item.put("rating", new AttributeValue(rating));
//        item.put("fans", new AttributeValue().withSS(fans));
//
//        return item;
//    }
    
}
