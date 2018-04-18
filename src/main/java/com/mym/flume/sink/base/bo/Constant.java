package com.mym.flume.sink.base.bo;

/**定值约定*/
public class Constant {
	
	/**文件组head信息中标示该文件组对应的processsor的headKey名称*/
	public static final String processorHeadKeyName = "processor";
	/**文件组head信息中在整个收集网络中 唯一 标示该文件组的headKey名称（例如：多台服务器上收集同为f1文件组的信息，通过此key标示f1属于哪台服务器上的f1）*/
	public static final String signalHeadKeyName = "signal";//配置在flume采集源上
	
}


