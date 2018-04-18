package com.mym.flume.sink.base;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DpcpConnectionPoolManager {

    private static DataSource dataSource = null;
    
    static {
    	
    }
    
    public static void init(Properties prop){
        try {
        	dataSource = BasicDataSourceFactory.createDataSource(prop);
        	System.out.println("success to init dpcp connection...");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    public static Connection getConnection() throws SQLException{
        return dataSource.getConnection();
    }

    public static void release(Connection conn){
        if (conn != null){
            try{
                conn.close();
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    
    public static void distoryConnectionPool(){
    	
    }
}
