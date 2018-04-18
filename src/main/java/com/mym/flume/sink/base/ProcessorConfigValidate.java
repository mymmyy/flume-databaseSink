package com.mym.flume.sink.base;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mym.flume.sink.base.bo.DefaultProcessConfig;

/**
 * 用于简单校验配置。校验内容：tableName、tableIdName
 * @author mym
 *
 */
public class ProcessorConfigValidate {
	
	public static void validateProcessorConfig(DefaultProcessConfig defaultProcessConfig){
		Connection connection = null;
		try {
			
			connection = DpcpConnectionPoolManager.getConnection();
			Statement createStatement = connection.createStatement();
			String checkSql = "select count("+defaultProcessConfig.getTableIdName()+") from "+defaultProcessConfig.getTableName();
			System.out.println(checkSql);
			createStatement.executeQuery(checkSql);
		} catch (SQLException e) {
			System.out.println("headValue as ["+defaultProcessConfig.getHeadValue()+"] config is fault! "+e.getMessage()+"。请�?查后重新启动�?");
			System.exit(0);
		}finally {
			if(connection!=null) {
				try {
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

}
