package com.mym.flume.sink.base.bo;

/**
 * 配置使用默认processor的配置信息
 * @author mym
 *
 */
public class DefaultProcessConfig {
	
	/**远程收集文件组的key*/
	private String headKey;
	
	/**远程收集文件组的value*/
	private String headValue;
	
	/**配置默认处理器对应的数据库表*/
	private String tableName;
	
	/**表的id�?*/
	private String tableIdName;
	
	private CRUDMethodType methodType;

	public String getHeadKey() {
		return headKey;
	}

	public void setHeadKey(String headKey) {
		this.headKey = headKey;
	}

	public String getHeadValue() {
		return headValue;
	}

	public void setHeadValue(String headValue) {
		this.headValue = headValue;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableIdName() {
		return tableIdName;
	}

	public void setTableIdName(String tableIdName) {
		this.tableIdName = tableIdName;
	}

	public CRUDMethodType getMethodType() {
		return methodType;
	}

	public void setMethodType(CRUDMethodType methodType) {
		this.methodType = methodType;
	}

	public DefaultProcessConfig(String headKey, String headValue, String tableName, String tableIdName,
			CRUDMethodType methodType) {
		super();
		this.headKey = headKey;
		this.headValue = headValue;
		this.tableName = tableName;
		this.tableIdName = tableIdName;
		this.methodType = methodType;
	}

	public DefaultProcessConfig(String headValue) {
		super();
		this.headValue = headValue;
	}
	
	
	
	

}
