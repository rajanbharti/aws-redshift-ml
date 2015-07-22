import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient;
import com.amazonaws.services.machinelearning.model.*;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.util.UUID;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.sql.*;

/**
 * Created by rajan on 7/17/15.
 */
public class AWSManager {
    private String access_key_id;
    private String access_key;
    private String bucketName;
    private String dbURL;
    private String dbUser;
    private String dbPass;
    private String dbName;
    private String clusterName;
    private Connection conn = null;
    private Statement stmt = null;
    private Properties properties = new Properties();
    private BasicAWSCredentials credentials;
    private AmazonMachineLearningClient machineLearningClient;
    private AmazonS3Client s3Client;
    private String outputUri;
    private String unloadPath;
    private String s3Source;
    private String mlRoleARN;
    private String mlStage;
    private String endPoint;

    AWSManager() {
        getConfig();
        credentials = new BasicAWSCredentials(access_key_id, access_key);
        machineLearningClient = new AmazonMachineLearningClient(credentials);
    }

    private String readProperties(String key) {
        String returnString = "";
        try {
            InputStream inputStream;
            inputStream = getClass().getClassLoader().getResourceAsStream("config.properties");
            properties.load(inputStream);
            returnString = properties.getProperty(key);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnString;
    }

    public void getConfig() {
        dbURL = readProperties("dbURL");
        dbName = readProperties("dbName");
        dbUser = readProperties("dbUser");
        dbPass = readProperties("dbPass");
        access_key = readProperties("accessKey");
        access_key_id = readProperties("accessKeyId");
        bucketName = readProperties("bucketName");
        s3Source = readProperties("s3Source");
        mlRoleARN = readProperties("mlRoleARN");
        clusterName = readProperties("clusterName");
        mlStage = readProperties("mlStage");
        outputUri = readProperties("outputUri");
        endPoint = readProperties("endPoint");
    }

    public void uploadToS3(String filePath, String fileId) {
        s3Client = new AmazonS3Client(credentials);
        File file = new File(filePath);
        PutObjectRequest objectRequest = new PutObjectRequest(bucketName, fileId, file);
        s3Client.putObject(objectRequest);
    }

    public ResultSet executeSQL(String sqlString)  {
        ResultSet resultSet=null;
        try {
            Class.forName("com.amazon.redshift.jdbc4.Driver");
            conn = DriverManager.getConnection(dbURL, dbUser, dbPass);
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(sqlString);
            resultSet.next();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return resultSet;
    }

    public void executeDDL(String ddlString) {
        try {
            Class.forName("com.amazon.redshift.jdbc4.Driver");
            conn = DriverManager.getConnection(dbURL, dbUser, dbPass);
            stmt = conn.createStatement();
            stmt.execute(ddlString);
            stmt.close();
            conn.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public void copyToRedshift(String tableName) {
        try {
            executeDDL("COPY " + tableName + " FROM '" + s3Source + "' credentials " +
                    "'aws_access_key_id=" + access_key_id + ";aws_secret_access_key=" + access_key + "';");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createRedShiftDataSource(String sqlQuery, String dataSourceId, String dataSchema) {

        RedshiftDatabase database = new RedshiftDatabase()
                .withDatabaseName(dbName)
                .withClusterIdentifier(clusterName);

        RedshiftDatabaseCredentials databaseCredentials = new RedshiftDatabaseCredentials()
                .withUsername(dbUser)
                .withPassword(dbPass);

        RedshiftDataSpec dataSpec = new RedshiftDataSpec()
                .withDatabaseInformation(database)
                .withDatabaseCredentials(databaseCredentials)
                .withSelectSqlQuery(sqlQuery)
                .withDataSchema(dataSchema)
                .withS3StagingLocation(mlStage);

        CreateDataSourceFromRedshiftRequest request = new CreateDataSourceFromRedshiftRequest()
                .withRoleARN(mlRoleARN)
                .withComputeStatistics(false)
                .withDataSourceId(dataSourceId)
                .withDataSpec(dataSpec);
        System.out.println(request.toString());
        machineLearningClient.createDataSourceFromRedshift(request);
    }

    public void createMLModel(String trainingDataSourceId, String modelId) {

        CreateMLModelRequest mlModelRequest = new CreateMLModelRequest()
                .withMLModelType(MLModelType.MULTICLASS)
                .withMLModelId(modelId)
                .withTrainingDataSourceId(trainingDataSourceId);
        System.out.println(mlModelRequest.toString());
        machineLearningClient.createMLModel(mlModelRequest);
    }

    public void predictBatchFromDSource(String dataSourceId,String modelId,String predictionId){
        CreateBatchPredictionRequest batchPredictionRequest = new CreateBatchPredictionRequest()
                .withMLModelId(modelId)
                .withBatchPredictionId(predictionId)
                .withBatchPredictionDataSourceId(dataSourceId)
                .withOutputUri(outputUri);

        machineLearningClient.createBatchPrediction(batchPredictionRequest);

        DeleteDataSourceRequest request = new DeleteDataSourceRequest()
                .withDataSourceId(dataSourceId);
        machineLearningClient.deleteDataSource(request);
    }
}

