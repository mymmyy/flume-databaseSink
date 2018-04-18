package com.mym.flume.sink.base;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mym.flume.sink.base.bo.CRUDMethodType;
import com.mym.flume.sink.base.bo.Constant;
import com.mym.flume.sink.base.bo.DefaultProcessConfig;

/**
 * 使用默认处理器的配置信息注册
 * @author mym
 *
 */
public class DefaultProcessConfigRegister {

	private static Log logger = LogFactory.getLog(DefaultProcessConfigRegister.class);
	
	private static String filename="defaultProcessorConfig.properties";
	
	private static ConcurrentHashMap<String,DefaultProcessConfig> defaultProcessConfigMap = new ConcurrentHashMap<String,DefaultProcessConfig>();
	
	public static void init(){
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		InputStream input = classLoader.getResourceAsStream(filename);
		
		if( input != null )
		{
			try
			{
				
				Properties properties = new Properties();
				properties.load(input);
				Set<Object> keySet = properties.keySet();
				//1.获取key，分析：headvalue.属性，进行归类
				Iterator<Object> iterator = keySet.iterator();
				while(iterator.hasNext()){
					String next = (String) iterator.next();
					String[] split = next.split("\\.");
					String value = properties.getProperty(next);
					//排除不正确的配置.直接抛出异常，停止服务
					if(split.length <= 1 || "".equals(value) || null == value || !Constant.processorHeadKeyName.equalsIgnoreCase(split[0])){
						throw new Exception("默认processor信息配置有误，请检查之后重启！");
					}
					String headValue = split[1];
					String prop = split[2];
					
					DefaultProcessConfig defaultProcessConfig = defaultProcessConfigMap.get(headValue);
					if(defaultProcessConfig == null){
						defaultProcessConfig = new DefaultProcessConfig(headValue);
						defaultProcessConfigMap.put(headValue, defaultProcessConfig);
					}
					
					
					if("tableName".equalsIgnoreCase(prop)){
						defaultProcessConfig.setTableName(value);
					}
					else if("tableIdName".equalsIgnoreCase(prop)){
						defaultProcessConfig.setTableIdName(value);
					}
					else if("methodType".equalsIgnoreCase(prop)){
						CRUDMethodType byName = CRUDMethodType.getByName(value);
						if(byName == null) {
							throw new Exception("默认processor信息配置有误：默认的processor指定错误，原因，不存在该默认的processor。请检查之后重启！");
						}
						defaultProcessConfig.setMethodType(byName);
					}
					else if("headKey".equalsIgnoreCase(prop)){//不是必须
						defaultProcessConfig.setHeadKey(value);
					}
					
					
					
					
					System.out.println("success load:"+next+":"+value);
				}
				
				//校验
				for(DefaultProcessConfig d:defaultProcessConfigMap.values()) {
					ProcessorConfigValidate.validateProcessorConfig(d);
				}
				System.out.println("success to complete load defaultProcessorConfig.properties!");
			} catch (FileNotFoundException e){
				logger.error("没找到defaultProcessorConfig.properties配置文件",e);
			} catch (IOException e){
				logger.error("读取defaultProcessorConfig.properties配置文件错误!",e);
			} catch (Exception e) {
				logger.error("读取defaultProcessorConfig.properties配置文件异常!",e);
			}
		}
		
	}
	
	public static void main(String[] args) {
		init();
		System.out.println(defaultProcessConfigMap);
	}

	public static ConcurrentHashMap<String, DefaultProcessConfig> getDefaultProcessConfigMap() {
		return defaultProcessConfigMap;
	}

	public static void setDefaultProcessConfigMap(
			ConcurrentHashMap<String, DefaultProcessConfig> defaultProcessConfigMap) {
		DefaultProcessConfigRegister.defaultProcessConfigMap = defaultProcessConfigMap;
	}
	
	
}
