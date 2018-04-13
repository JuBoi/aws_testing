

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.DescribeMetricFiltersRequest;
import com.amazonaws.services.logs.model.DescribeMetricFiltersResult;
import com.amazonaws.services.logs.model.FilterLogEventsRequest;
import com.amazonaws.services.logs.model.FilterLogEventsResult;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.*;
import com.amazonaws.services.cloudwatch.*;
import com.amazonaws.services.cloudwatch.model.*;

import java.util.*;
import java.sql.*;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class AWS {

    private static final int N = 8;

    private static final int PERIOD = 120;
    private static final int MINUTES = 30;
    private static final long OFFSETINMILLISECONDS = 1000 * 60 * MINUTES;
    

    private static final String DBINSTANCE = "bjutestpostgres";

    private static final AmazonCloudWatch cw = AmazonCloudWatchClientBuilder.defaultClient();
    private static final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();
    private static final AWSLogs logClient = AWSLogsClientBuilder.standard().build();

    private static void executeStatement(Connection conn, String sqlStatement) throws SQLException {
        System.out.println("I be executin");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sqlStatement);

//        while(rs.next()){
//            System.out.println(rs.getString(1)
//                + " " + rs.getString(2)
//                + " " + rs.getString(3));
//        }
    }

    private static void connectToDatabase(String url, String sqlStatement){
        try (Connection conn = DriverManager.getConnection(url, USERNAME, PASSWORD)){
            System.out.println("Successful connection!");
            for(int i = 0; i < 20; i++) {
                executeStatement(conn, sqlStatement);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void listMetricData(String metricName){
        GetMetricStatisticsRequest getMetricsRequest = new GetMetricStatisticsRequest()
                .withStartTime(new Date(new Date().getTime() - OFFSETINMILLISECONDS))
                .withPeriod(PERIOD)
                .withNamespace("AWS/RDS")
                .withMetricName(metricName)
                .withStatistics("Minimum", "Average", "Maximum")
                .withEndTime(new Date());

        GetMetricStatisticsResult getMetricStatisticsResult = cw.getMetricStatistics(getMetricsRequest);
        List<Datapoint> datapoints = getMetricStatisticsResult.getDatapoints();
        Collections.sort(datapoints, (d1, d2) -> {
            if(d1 == null || d2 == null) return 0;
            return d1.getTimestamp().compareTo(d2.getTimestamp());
        });
        for(Datapoint data : datapoints) {
            System.out.println(data.toString());
        }
    }

    private static void getLog(String logFile) throws InterruptedException {
        DownloadDBLogFilePortionRequest downloadDBLogFilePortionRequest = new DownloadDBLogFilePortionRequest()
                .withDBInstanceIdentifier(DBINSTANCE)
                .withLogFileName(logFile)
                .withNumberOfLines(100);
        DownloadDBLogFilePortionResult downloadDBLogFilePortionResult = rdsClient.downloadDBLogFilePortion(downloadDBLogFilePortionRequest);
        System.out.println(downloadDBLogFilePortionResult.getLogFileData());
        System.out.println("----------------------------------");
        System.out.println("MARKER");
        System.out.println("----------------------------------");
        String marker = downloadDBLogFilePortionResult.getMarker();
        TimeUnit.SECONDS.sleep(60);
        System.out.println(marker);
        DownloadDBLogFilePortionRequest downloadDBLogFilePortionRequest1 = new DownloadDBLogFilePortionRequest()
                .withDBInstanceIdentifier(DBINSTANCE)
                .withLogFileName("audit/server_audit.log")
                .withMarker(marker)
                .withNumberOfLines(100);
        DownloadDBLogFilePortionResult downloadDBLogFilePortionResult1 = rdsClient.downloadDBLogFilePortion(downloadDBLogFilePortionRequest1);
        System.out.println(downloadDBLogFilePortionResult1.getLogFileData());
    }

    private static OptionSetting createOPSetting(String name, String value){
        return new OptionSetting().withName(name).withValue(value);
    }

    private static OptionGroup createOPGroup(String engineName, String engineVersion, String description, String name){
        CreateOptionGroupRequest createOptionGroupRequest = new CreateOptionGroupRequest()
                .withEngineName(engineName)
                .withMajorEngineVersion(engineVersion)
                .withOptionGroupDescription(description)
                .withOptionGroupName(name);
        return rdsClient.createOptionGroup(createOptionGroupRequest);
    }

    private static OptionConfiguration addOPSettings(){
        OptionSetting[] auditOPSettings = new OptionSetting[3];

        auditOPSettings[0] = createOPSetting("SERVER_AUDIT_FILE_ROTATIONS", "10");
        auditOPSettings[1] = createOPSetting("SERVER_AUDIT_EVENTS", "CONNECT,QUERY,TABLE");
        auditOPSettings[2] = createOPSetting("SERVER_AUDIT_FILE_ROTATE_SIZE", "1000000");

        OptionConfiguration auditOP = new OptionConfiguration()
                .withOptionName("MARIADB_AUDIT_PLUGIN")
                .withOptionSettings(auditOPSettings);

        return auditOP;
    }

    private static OptionGroup modifyGroup(OptionConfiguration auditOP){
        ModifyOptionGroupRequest modifyOptionGroupRequest = new ModifyOptionGroupRequest()
                .withApplyImmediately(true)
                .withOptionGroupName("testOP")
                .withOptionsToInclude(auditOP);

        return rdsClient.modifyOptionGroup(modifyOptionGroupRequest);
    }

    private static void testOptionGroup(){
        try {
            createOPGroup("mysql", "5.6", "test OP", "testOP");
            OptionConfiguration auditOP = addOPSettings();
            OptionGroup newGroup = modifyGroup(auditOP);
            System.out.println(newGroup.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createParameterGroup(){
        CreateDBParameterGroupRequest createDBParameterGroupRequest = new CreateDBParameterGroupRequest()
                .withDBParameterGroupName("testParameterGroup")
                .withDescription("test parameter group")
                .withDBParameterGroupFamily("mysql5.6");
        DBParameterGroup testParameterGroup = rdsClient.createDBParameterGroup(createDBParameterGroupRequest);
    }

    private static Parameter createParameterSetting(String name, String value){
        return new Parameter().withParameterName(name).withParameterValue(value).withApplyMethod("immediate");
    }

    private static ModifyDBParameterGroupResult modifyParameterGroup(String name){
        Parameter[] parameters = new Parameter[2];
        parameters[0] = createParameterSetting("slow_query_log", "1");
        parameters[1] = createParameterSetting("general_log", "1");

        ModifyDBParameterGroupRequest modifyDBParameterGroupRequest = new ModifyDBParameterGroupRequest()
                .withDBParameterGroupName(name)
                .withParameters(parameters);

        return rdsClient.modifyDBParameterGroup(modifyDBParameterGroupRequest);
    }

    private static void testParameterGroup(){
        //createParameterGroup();
        ModifyDBParameterGroupResult wut = modifyParameterGroup("testParameterGroup");
        System.out.println(wut.toString());
    }

    private static DBInstance modifyDB(){
        ModifyDBInstanceRequest modifyDBInstanceRequest = new ModifyDBInstanceRequest()
                .withApplyImmediately(true)
                .withDBInstanceIdentifier(DBINSTANCE)
//                .withOptionGroupName("testOP")
//                .withDBParameterGroupName("testParameterGroup")
                .withMonitoringInterval(30);
        return rdsClient.modifyDBInstance(modifyDBInstanceRequest);
    }

    private static void filterRequest(){
        FilterLogEventsRequest filterLogEventsRequest = new FilterLogEventsRequest()
                .withLogGroupName("/aws/rds/instance/bjutestmysql/audit")
                .withFilterPattern("Champions");
        FilterLogEventsResult logResults = logClient.filterLogEvents(filterLogEventsRequest);
        System.out.println(logResults.toString());
    }

    private static void createExampleDB(){
        CreateDBInstanceRequest request = new CreateDBInstanceRequest()
                .withDBInstanceIdentifier("exampleinstance")
                .withAllocatedStorage(20)
                .withDBInstanceClass("db.t2.micro")
                .withEngine("MySQL")
                .withMasterUsername("master")
                .withMasterUserPassword("masterPassword");
        DBInstance exampleDB = rdsClient.createDBInstance(request);
    }

    public static void main(String[] args) throws InterruptedException {

//        listMetricData("CPUUtilization");
//
//        getLog("audit/server_audit.log");
//
//        String databaseUrl = "jdbc:mysql://" + MYSQLSERVERURL +
//                ":3306/" + "sakila";
//
//        for(int i = 0; i < N; i++){
//            new Thread(() -> {
//                connectToDatabase(databaseUrl, "SELECT * FROM city");
//            }).start();
//        }
//        testOptionGroup();
//        testParameterGroup();

//        DBInstance testInstance = modifyDB();
//        filterRequest();

        createExampleDB();
    }
}
