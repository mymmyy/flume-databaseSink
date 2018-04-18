package com.mym.flume.sink.base;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mym.flume.sink.base.bo.Constant;
/**
 * @author mym
 */
public class CustomerProcessorConfigRegister {
	
	private static Log logger = LogFactory.getLog(CustomerProcessorConfigRegister.class);
	
	private static String filename="customerProcessorConfig.properties";
	
	//headKey对应的headValue值与processor的className的Map
	private static ConcurrentHashMap<String, String> headValueProcessorMap = new ConcurrentHashMap<String, String>(); 

	//存储headKey�?
	private static Set<String> headKeySet = new HashSet<String>();
	
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
				//1.获取key，分析：headvalue.属�?�，进行归类
				Iterator<Object> iterator = keySet.iterator();
				while(iterator.hasNext()){
					String next = (String) iterator.next();
					String[] split = next.split("\\.");
					String value = properties.getProperty(next);
					//排除不正确的配置.直接抛出异常，停止服�?
					if(split.length <= 1 || "".equals(value) || null == value || !Constant.processorHeadKeyName.equalsIgnoreCase(split[0])){
						throw new Exception("自定义的processor信息配置有误，请�?查之后重启！");
					}
					
					String headValue = split[1];
					headValueProcessorMap.put(headValue, value);
					String headKey = split[0];
					headKeySet.add(headKey);
					
					System.out.println("success load:"+next+":"+value);
				}
				System.out.println("success to complete load processorConfig.properties!");
			} catch (FileNotFoundException e){
				logger.error("没找到customerProcessorConfigRegister.properties配置文件",e);
			} catch (IOException e){
				logger.error("读取customerProcessorConfigRegister.properties配置文件错误!",e);
			} catch (Exception e) {
				logger.error("读取customerProcessorConfigRegister.properties配置文件异常!",e);
			}
		}
		
	}
	
	public static ConcurrentHashMap<String, String> getHeadValueProcessorMap() {
		return headValueProcessorMap;
	}

	public static Set<String> getHeadKeySet() {
		return headKeySet;
	}


}
