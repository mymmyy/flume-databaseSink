package com.mym.flume.sink.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.flume.Event;

import com.google.common.collect.Lists;
import com.mym.flume.sink.base.bo.Constant;
import com.mym.flume.sink.base.bo.DefaultProcessConfig;
import com.mym.flume.sink.processor.base.AbstractEventProcessor;

public class DefaultInsertEventProcessor extends AbstractEventProcessor {
	
	private Map<String, String> inshortContentMap = new HashMap<String, String>();//处理非连续数据：打印�?半数据�?�下�?次打印另�?半数�?
	
	private DefaultProcessConfig defaultProcessConfig = null;
	
	public void init(DefaultProcessConfig defaultProcessConfig){
		this.defaultProcessConfig = defaultProcessConfig;
	}
	
	public DefaultInsertEventProcessor(DefaultProcessConfig defaultProcessConfig) {
		init(defaultProcessConfig);
	}
	
	@Override
	protected Connection eventProcess(ArrayList<Event> eventList,
			Connection connection) throws SQLException {
		if(eventList == null || eventList.size() <= 0){
			return connection;
		}
		//1.拿到第一条数据的�?有key，对应表的字段（前提，每条数据的key�?致，否则出错）�?�key不一致的不�?�合默认processor
		Event event = eventList.get(0);
		List<String> jsonKeys = this.getJsonKeys(this.eventBodyToJson(event.getBody()));
		
		//2.得到预处理对�?
		PreparedStatement preparedStatement = this.getInsertPreparedStatement(jsonKeys, defaultProcessConfig.getTableName(), connection);
		//3.处理eventContent内容*/
		List<JSONObject> josnObjs = Lists.newArrayList();
		for(Event e:eventList){
			//3.1 处理有残缺的信息：当log4j�?启缓存的时�?�，�?后打印的�?条数据可能是不完整的，先存储，等待另�?半过来再处理
			String content = new String(e.getBody());
			if(content.startsWith("{") && !content.endsWith("}")){
				inshortContentMap.put(e.getHeaders().get(Constant.signalHeadKeyName), content);
				continue;
			}
			if(!content.startsWith("{") && content.endsWith("}")){
				String str = inshortContentMap.get(e.getHeaders().get(Constant.signalHeadKeyName));
				if(str != null && !"".equals(str)){
					content = str + content;
				}else{
					continue;
				}
			}
			if(!content.startsWith("{") && !content.endsWith("}")){
				continue;
			}
			
			//3.2 正真处理信息:对于中文乱码有可能在此出现异常导致整个服务死�?
			JSONObject fromObject = JSONObject.fromObject(content);
			if(fromObject != null){
				josnObjs.add(fromObject);
			}
		}
		
		/*4.jdbc提交*/
        this.baseDefaultJdbcExecuteBatchInsert(connection, preparedStatement, josnObjs, jsonKeys);
		return connection;
	}
	
	@Override
	protected String getProcessorHeadValue() {
		return this.defaultProcessConfig.getHeadValue();
	}

	@Override
	protected Connection eventProcess(ArrayList<String> eventContent,
			String headValue, Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public static void main(String[] args) {
		String str="{'card_set':-1,'change_after':1370041";
		System.out.println(str.endsWith("}"));
	}
	


}
