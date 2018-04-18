package com.mym.flume.sink.base;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import com.mym.flume.sink.base.bo.DefaultProcessConfig;
import com.mym.flume.sink.processor.DefaultInsertEventProcessor;
import com.mym.flume.sink.processor.base.AbstractEventProcessor;

/**
 * 事件处理器的管理中心
 * @author mym
 *
 */
public class ProcessorPoolManager {

	/**自定义事件处理器的单例map*/
	public static ConcurrentHashMap<String, AbstractEventProcessor> processorSingleInstanceMap = new ConcurrentHashMap<String, AbstractEventProcessor>();
	
	/**
	 * 初始化管理中心的数据
	 * @param eventMap 
	 */
	public static void init(){
    	// 加载processorConfig配置文件
		CustomerProcessorConfigRegister.init();

		// 加载defaultProcessorConfig配置文件
		DefaultProcessConfigRegister.init();
		
		//自定义的processsor实例初始�?
		int num = 0;
        ConcurrentHashMap<String,String> headValueProcessorMap = CustomerProcessorConfigRegister.getHeadValueProcessorMap();
        Iterator<String> iterator = headValueProcessorMap.keySet().iterator();
        while(iterator.hasNext()){
        	String headValue = iterator.next();
        	AbstractEventProcessor newInstance = null;
			try {
				newInstance = (AbstractEventProcessor) Class.forName(headValueProcessorMap.get(headValue)).newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
			if(newInstance != null){
				processorSingleInstanceMap.put(headValue, newInstance);
			}
			
			System.out.println("init the "+ ++num+" processor:"+newInstance.getClass().getName());
        }
		
        
        //默认processor实例初始�?
        ConcurrentHashMap<String,DefaultProcessConfig> defaultProcessConfigMap = DefaultProcessConfigRegister.getDefaultProcessConfigMap();
        Iterator<String> defaultIterator = defaultProcessConfigMap.keySet().iterator();
        while(defaultIterator.hasNext()) {
        	String next = defaultIterator.next();
        	DefaultProcessConfig defaultProcessConfig = defaultProcessConfigMap.get(next);
        	
        	DefaultInsertEventProcessor defaultInsertEventProcessor = new DefaultInsertEventProcessor(defaultProcessConfig);
        	processorSingleInstanceMap.put(defaultProcessConfig.getHeadValue(), defaultInsertEventProcessor);
        	
        	System.out.println("init the "+ ++num+" processor:"+defaultInsertEventProcessor.getClass().getName());
        }
        
		
		
	}

	public static ConcurrentHashMap<String, AbstractEventProcessor> getProcessorSingleInstanceMap() {
		return processorSingleInstanceMap;
	}
	
}
