package com.mym.flume.sink.processor.base;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.flume.Event;

import com.mym.flume.sink.base.DpcpConnectionPoolManager;

/**
 * event处理器
 * @author mym
 *
 */
public abstract class AbstractEventProcessor {
	
	protected abstract Connection eventProcess(ArrayList<String> eventContent,String headValue, Connection connection) throws SQLException;
	
	protected abstract Connection eventProcess(ArrayList<Event> eventList, Connection connection) throws SQLException;
	
	/**获取该文件组对应processor的headKeyName*/
	protected abstract String getProcessorHeadValue();
	
	public AbstractEventProcessor(){
	}
	
	public Connection baseEventProcessor(ArrayList<String> eventContent,String headValue, Connection connection) throws SQLException{
		return this.eventProcess(eventContent, headValue, connection);
	}
	
	protected PreparedStatement getInsertPreparedStatement(Connection conn, String tableName, Class clazz){
    	if(clazz == null){
    		return null;
    	}
    	
    	PreparedStatement preparedStatement = null;
    	Field[] fields = clazz.getDeclaredFields();
    	StringBuffer propStringBuffer = new StringBuffer();
    	StringBuffer _stringBuffer = new StringBuffer();
    	for(Field filed:fields){
    		filed.setAccessible(true);
    		propStringBuffer.append(filed.getName()).append(",");
    		_stringBuffer.append("?,");
    	}
    	
    	String propString = propStringBuffer.substring(0, propStringBuffer.lastIndexOf(","));
    	String _string = _stringBuffer.substring(0, _stringBuffer.lastIndexOf(","));
		try {
			preparedStatement = conn.prepareStatement("insert into " + tableName +
					" ("+propString+") values ("+_string+")");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	
    	return preparedStatement;
    }
	
	/**
	 * 
	 * @param propName 属性名列表
	 * @param tableName 表名
	 * @return
	 * @throws SQLException 
	 */
	protected PreparedStatement getInsertPreparedStatement(List<String> propNames, String tableName, Connection conn) throws SQLException{
		PreparedStatement preparedStatement = null;

		if(propNames == null || propNames.size() <= 0 || "".equalsIgnoreCase(tableName)){
			return preparedStatement;
		}
		
    	StringBuffer propStringBuffer = new StringBuffer();
    	StringBuffer _stringBuffer = new StringBuffer();
    	for(String prop:propNames){
    		propStringBuffer.append(prop).append(",");
    		_stringBuffer.append("?,");
    	}
    	
    	String propString = propStringBuffer.substring(0, propStringBuffer.lastIndexOf(","));
    	String _string = _stringBuffer.substring(0, _stringBuffer.lastIndexOf(","));
		preparedStatement = conn.prepareStatement("insert into " + tableName +
					" ("+propString+") values ("+_string+")");
	
    	return preparedStatement;
	}
	
	
	/**
	 * 获取根据主键进行更新的PreparedStatement
	 * @param conn
	 * @param tableName
	 * @param clazz
	 * @param primarykeyName 表的 主键名，根据主键更新
	 * @return
	 */
//	protected PreparedStatement getUpdatePreparedStatement(Connection conn, String tableName, Class clazz, String primarykeyName){
//    	if(clazz == null){
//    		return null;
//    	}
//    	
//    	PreparedStatement preparedStatement = null;
//    	Field[] fields = clazz.getDeclaredFields();
//    	StringBuffer propStringBuffer = new StringBuffer();
//    	propStringBuffer.append(" set ");
//    	for(Field filed:fields){
//    		filed.setAccessible(true);
//    		propStringBuffer.append(filed.getName()).append(" = ? ,");
//    	}
//    	
//    	String propString = propStringBuffer.substring(0, propStringBuffer.lastIndexOf(","));
//		try {
//			preparedStatement = conn.prepareStatement("update  " + tableName +
//					" "+propString+" where  primarykeyName = ?");
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	
//    	return preparedStatement;
//    }
	
	/**进行json数据集持久化?.注：json数据key必须与clazz对象的属性具有一一对应关系。否则请另行实现自定义持久化方法
	 * @throws SQLException */
	protected void baseJdbcExecuteBatchInsert(Connection connection, PreparedStatement preparedStatement, List<JSONObject> josnObjs, Class clazz) throws SQLException {
		if(josnObjs== null || josnObjs.size() <= 0){
			return;
		}
		connection.setAutoCommit(false);
		preparedStatement.clearBatch();
		for (JSONObject temp : josnObjs) {
			Field[] fields = clazz.getDeclaredFields();
			int i = 0;
			for(Field field:fields){
				field.setAccessible(true);
				preparedStatement.setObject(++i, temp.get(field.getName()));
			}
			preparedStatement.addBatch();
		}
		preparedStatement.executeBatch();
		preparedStatement.close();
	}
	
	/**进行json数据集�?�持久化插入�?.注：json数据key必须与clazz对象的属性具有一�?对应关系。否则请另行实现自定义持久化方法
	 * @throws SQLException */
	protected void baseDefaultJdbcExecuteBatchInsert(Connection connection, PreparedStatement preparedStatement, List<JSONObject> josnObjs,List<String> props) throws SQLException {
		if(josnObjs== null || josnObjs.size() <= 0){
			return;
		}
		connection.setAutoCommit(false);
		preparedStatement.clearBatch();
		for (JSONObject temp : josnObjs) {
			int i = 0;
			for(String str:props){
				preparedStatement.setObject(++i, temp.get(str));
			}
			preparedStatement.addBatch();
		}
		preparedStatement.executeBatch();
		preparedStatement.close();
	}
	
	
	/**进行json数据集�?�持久化更新】根据主键更�?.注：json数据key必须与clazz对象的属性具有一�?对应关系。否则请另行实现自定义持久化方法
	 * @param string 
	 * @param class1 
	 * @param josnObjs */
//	protected void baseJdbcExecuteBatchUpdate(Connection connection, PreparedStatement preparedStatement, Collection<JSONObject> josnObjs, Class clazz, String primaryKeyName) {
//		try {
//			connection.setAutoCommit(false);
//			preparedStatement.clearBatch();
//			for (JSONObject temp : josnObjs) {
//				Field[] fields = clazz.getDeclaredFields();
//				int i = 0;
//				for(Field field:fields){
//					field.setAccessible(true);
//					preparedStatement.setObject(++i, temp.get(field.getName()));
//				}
//				preparedStatement.setObject(++i, temp.get(primaryKeyName));//设置�?后一个主�??�?
//				preparedStatement.addBatch();
//			}
//			preparedStatement.executeBatch();
//			preparedStatement.close();
////				connection.commit();
//		} catch (SQLException e) {
////				try {
////					connection.rollback();
////				} catch (SQLException e1) {
////					e1.printStackTrace();
////				}
//			e.printStackTrace();
//		}
//	}
	
	/**获得普通连接：连接将不会包含在processor事务
	 * @throws SQLException */
	protected Connection getDefaultConnection() throws SQLException{
		return DpcpConnectionPoolManager.getConnection();
	}

	public Connection baseEventProcessor(ArrayList<Event> eventlist, Connection connection) throws SQLException {
		return this.eventProcess(eventlist, connection);
	}
	
	/**event消息体转成json类型数据*/
	protected JSONObject eventBodyToJson(byte[] bytes){
		System.out.println("-----:"+bytes);
		String string = new String(bytes);
		System.out.println("ABSTRA:"+string);
		return JSONObject.fromObject(string);
	}
	
	/**event消息体转成String类型数据*/
	protected String eventBodyToString(byte[] bytes){
		return new String(bytes);
	}
	
	/**获取json的所有key*/
	protected List<String> getJsonKeys(JSONObject json){
		List<String> jsonKeys = new ArrayList<String>();
		if(json == null){
			return jsonKeys;
		}
		Iterator<String> keys = json.keys();
		while(keys.hasNext()){
			jsonKeys.add((String) keys.next());
		}
		return jsonKeys;
		
	}
	
}
