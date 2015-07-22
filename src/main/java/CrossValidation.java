import com.amazonaws.services.machinelearning.model.GetDataSourceRequest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by rajan on 7/21/15.
 */
public class CrossValidation {
    private AWSManager awsManager = new AWSManager();
    private String tableName;
    private String columnList;          //column list separated by commas(,)
    private String dataSchema;
    private int recordSize;
    private Date date = new Date();

    CrossValidation(String tableName, String columnList, String dataSchema) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.dataSchema = dataSchema;
        recordSize = getRecordSize(tableName);
        System.out.println(recordSize);
    }


    public List<String> CVRandom(int k, int trainingSize, long seed) {
        ArrayList<String> dataSourceList = new ArrayList<String>();

        int resultSize = recordSize / trainingSize;
        for (int i = 0; i < k; i++) {
            String dataSourceId = "ddf-" + date.getTime() + i;
            String tempView="temp"+date.getTime()+i;

            awsManager.executeDDL("CREATE VIEW "+tempView+" AS SELECT " + columnList + " FROM " + tableName +
                    " ORDER BY RANDOM() LIMIT " + resultSize + ";");
            awsManager.createRedShiftDataSource("SELECT * FROM "+tempView+ ";", dataSourceId, dataSchema);

            dataSourceList.add(dataSourceId);
        }
        return dataSourceList;
    }

    public List<String> CVK(int k,long seed){
        ArrayList<String> dataSourceList=new ArrayList<String>();
        int recordSize=getRecordSize(tableName);
        int resultSize=recordSize/k;
        awsManager.executeSQL("CREATE OR REPLACE VIEW randomView AS SELECT * from "+tableName+"ORDER BY RANDOM();");
        for(int i=0;i<k;i++){
            String dataSourceId = "ddf-" + date.getTime() + i;
            String tempView="temp"+date.getTime()+i;

            awsManager.executeDDL("CREATE VIEW "+tempView+" AS SELECT " + columnList + " FROM " + tableName +
                    " ORDER BY RANDOM() LIMIT " + resultSize + ";");
            awsManager.createRedShiftDataSource("SELECT * FROM "+tempView+ ";", dataSourceId, dataSchema);

            dataSourceList.add(dataSourceId);
        }


        return dataSourceList;
    }

    private int getRecordSize(String tableName) {
        int recordSize = 0;
        try {
            recordSize = awsManager.executeSQL("SELECT COUNT(*) FROM " + tableName + ";").getInt("COUNT");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recordSize;
    }

}
