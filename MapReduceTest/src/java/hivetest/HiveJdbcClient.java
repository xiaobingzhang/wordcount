package hivetest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcClient {
	private static String dirverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive://192.168.7.128:10000/default";
	private static String user = "";
	private static String password = "";
	public static void main(String[] args) {
		try {
			Class.forName(dirverName);
			Connection con = DriverManager.getConnection(url, user, password);
			Statement stmt = con.createStatement();
			String tableName = "testHiveDriverTable";
			stmt.executeQuery("drop table " + tableName);
			ResultSet res = stmt.executeQuery("create table " + tableName
					+ " (key int, value string)");
			// show tables
			String sql = "show tables '" + tableName + "'";
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			if (res.next()) {
				System.out.println(res.getString(1));
			}
			// describe table
			sql = "describe " + tableName;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2));
			}

			// load data into table
			// NOTE: filepath has to be local to the hive server
			// NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per
			// line
			String filepath = "/opt/hive/tmp/a.txt";
			sql = "load data local inpath '" + filepath + "' into table "
					+ tableName;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);

			// select * query
			sql = "select * from " + tableName;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(String.valueOf(res.getInt(1)) + "\t"
						+ res.getString(2));
			}

			// regular hive query
			sql = "select count(1) from " + tableName;
			System.out.println("Running: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}


/*
# Then on the command-line
$ javac HiveJdbcClient.java
 
# To run the program in standalone mode, we need the following jars in the classpath
# from hive/build/dist/lib
#     hive_exec.jar
#     hive_jdbc.jar
#     hive_metastore.jar
#     hive_service.jar
#     libfb303.jar
#     log4j-1.2.15.jar
#
# from hadoop/build
#     hadoop-*-core.jar
#
# To run the program in embedded mode, we need the following additional jars in the classpath
# from hive/build/dist/lib
#     antlr-runtime-3.0.1.jar
#     derby.jar
#     jdo2-api-2.1.jar
#     jpox-core-1.2.2.jar
#     jpox-rdbms-1.2.2.jar
#
# as well as hive/build/dist/conf
 
$ java -cp $CLASSPATH HiveJdbcClient
 
# Alternatively, you can run the following bash script, which will seed the data file
# and build your classpath before invoking the client.
 
#!/bin/bash
HADOOP_HOME=/your/path/to/hadoop
HIVE_HOME=/your/path/to/hive
 
echo -e '1\x01foo' > /tmp/a.txt
echo -e '2\x01bar' >> /tmp/a.txt


 
HADOOP_CORE=/opt/hadoop/hadoop-core-1.2.1.jar
CLASSPATH=.:$HADOOP_CORE:$HIVE_HOME/conf
 
for i in ${HIVE_HOME}/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

 
java -cp $CLASSPATH HiveJdbcClient
 */
