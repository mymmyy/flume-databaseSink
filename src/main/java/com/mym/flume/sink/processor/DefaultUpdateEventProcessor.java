package com.mym.flume.sink.processor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

import com.mym.flume.sink.base.bo.DefaultProcessConfig;
import com.mym.flume.sink.processor.base.AbstractEventProcessor;

public class DefaultUpdateEventProcessor extends AbstractEventProcessor {
	
	private Map<String, String> inshortContentMap = new HashMap<String, String>();//处理非连续数据：打印�?半数据�?�下�?次打印另�?半数�?
	
	private DefaultProcessConfig defaultProcessConfig = null;

	public void init(DefaultProcessConfig defaultProcessConfig){
		this.defaultProcessConfig = defaultProcessConfig;
	}

	@Override
	protected Connection eventProcess(ArrayList<String> eventContent, String headValue, Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Connection eventProcess(ArrayList<Event> eventList, Connection connection) throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getProcessorHeadValue() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
