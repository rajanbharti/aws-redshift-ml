import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by rajan on 7/21/15.
 */
public class CrossValidation {
    private AWSManager awsManager = new AWSManager();
    private String tableName;
    private String columnList;          //column list separated by commas(,)
    private String dataSchema;
    private int rowCount;
    private Date date = new Date();

    CrossValidation(String tableName, String columnList, String dataSchema) {
        this.tableName = tableName;
        this.columnList = columnList;
        this.dataSchema = dataSchema;
        rowCount = getRowCount(tableName);
        System.out.println(rowCount);
    }


    public List<String> CVRandom(int k, int trainingSize, long seed) {
        ArrayList<String> dataSourceList = new ArrayList<String>();

        int resultSize = rowCount / trainingSize;
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
        int recordSize= getRowCount(tableName);
        int resultSize=recordSize/k;

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

    private int getRowCount(String tableName) {
        int rowCount = 0;
        try {
            rowCount = awsManager.executeSQL("SELECT COUNT(*) FROM " + tableName + ";").getInt("COUNT");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rowCount;
    }

}
