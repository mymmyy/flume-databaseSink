package com.mym.flume.sink.kernel;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.mym.flume.sink.base.DpcpConnectionPoolManager;
import com.mym.flume.sink.base.ProcessorPoolManager;
import com.mym.flume.sink.base.bo.Constant;
import com.mym.flume.sink.processor.base.AbstractEventProcessor;

/**
 * 
 * @author mym
 *
 */
public class BaseAdminMysqlSink extends AbstractSink implements Configurable {

    private Logger LOG = LoggerFactory.getLogger(BaseAdminMysqlSink.class);
    
    private ReentrantLock lock = new ReentrantLock();
    
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);
    
	private int batchSize;
	
	private Properties dpcpProp = null;
	
    private ConcurrentHashMap<String, Connection> usingConnections = new ConcurrentHashMap<String, Connection>();
    
    /**key=processor对应的value，value为消息集合*/
    private ConcurrentHashMap<String, ArrayList<Event>> eventMap = new ConcurrentHashMap<String, ArrayList<Event>>();
    
    private AtomicBoolean isSuccessProcess = new AtomicBoolean(true);//默认为true。只要有一个为false，则全部回滚
    
    public BaseAdminMysqlSink() {
        
    }
    
    /**实现Configurable可以获取配置文件中的配置数据*/
    public void configure(Context context) {
        String jdbcUrl;
        String username;
        String password;
        String driverClassName;
        int initialSize=GenericObjectPool.DEFAULT_MIN_IDLE;		
        int maxActive=GenericObjectPool.DEFAULT_MAX_ACTIVE;		//池中最多可容纳的活着的连接数量
        int maxIdle=GenericObjectPool.DEFAULT_MAX_IDLE;			//maxIdle的意思是连接池最多可以保持的连接数
        int minIdle=GenericObjectPool.DEFAULT_MIN_IDLE;			
        long maxWait=GenericObjectPool.DEFAULT_MAX_WAIT;		//获取连接的等待时间，如果超过这个时间则抛出异常（默认配置）

        /* 高可用配置：
         * 	connectionInitSqls
        	testOnBorrow
        	testOnReturn
        	validationQuery
        	validationQueryTimeout
         * connectionInitSqls是在一个connection被创建之后调用的一组sql，这组sql可用来记录日志，也可以用来初始化一些数据，
         * 总之，经过connectionInitSqls之后的connection一定是正常可用的。
         * testOnBorrow和testOnReturn的关注点则在connection从池中“取出”和“归还”，这两个关键的动作上，当他们被设置为true时，在取出和归还connection时，都需要完成校验，
         * 如果校验不通过，这个connection将被销毁。校验的sql由validationQuery定义，且定义的sql语句必须是查询语句，而且查询至少一列。
         * validationQueryTimeout定义的是校验查询时长，如果超过这个时间，则认定为校验失败
         */
    
        driverClassName = context.getString("driverClassName");
        Preconditions.checkNotNull(driverClassName, "driverClassName must be set!!");
    	jdbcUrl = context.getString("jdbcUrl");
        Preconditions.checkNotNull(jdbcUrl, "jdbcUrl must be set!!");
        username = context.getString("username");
        Preconditions.checkNotNull(username, "user must be set!!");
        password = context.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        
        String _initialSize = context.getString("initialSize");
        initialSize = _initialSize == null || "".equals(_initialSize) ? initialSize : Integer.parseInt(_initialSize);
        String _maxActive = context.getString("maxActive");
        maxActive = _maxActive == null || "".equals(_maxActive) ? maxActive : Integer.parseInt(_maxActive);
        String _maxIdle = context.getString("maxIdle");
        maxIdle = _maxIdle == null || "".equals(_maxIdle) ? maxIdle : Integer.parseInt(_maxIdle);
        String _minIdle = context.getString("minIdle");
        minIdle = _minIdle == null || "".equals(_minIdle) ? minIdle : Integer.parseInt(_minIdle);
        String _maxWait = context.getString("maxWait");
        maxWait = _maxWait == null || "".equals(_maxWait) ? maxWait : Integer.parseInt(_maxWait);
        
        batchSize = context.getInteger("batchSize", 100);		
        if(dpcpProp == null){
        	dpcpProp = new Properties();
        }
        dpcpProp.setProperty("driverClassName", driverClassName);
        dpcpProp.setProperty("url", jdbcUrl);
        dpcpProp.setProperty("username", username);
        dpcpProp.setProperty("password", password);
        
        dpcpProp.setProperty("initialSize", String.valueOf(initialSize));
        dpcpProp.setProperty("maxActive", String.valueOf(maxActive));
        dpcpProp.setProperty("maxIdle", String.valueOf(maxIdle));
        dpcpProp.setProperty("minIdle", String.valueOf(minIdle));
        dpcpProp.setProperty("maxWait", String.valueOf(maxWait));
    }

    /**
     * 服务启动时执行的代码，一般做准备工作
     */
    @Override
    public void start() {
    	super.start();
    	LOG.info("BaseAdminMysqlSink start...");
        //初始化连接池
        DpcpConnectionPoolManager.init(dpcpProp);
    	// 初始化处理器
    	ProcessorPoolManager.init();
    	
    	// 初始化eventMap
    	Set<String> keySet = ProcessorPoolManager.getProcessorSingleInstanceMap().keySet();
    	Iterator<String> iterator = keySet.iterator();
    	int num = 0;
    	while(iterator.hasNext()) {
    		String headValue = iterator.next();
    		this.eventMap.put(headValue, new ArrayList<Event>());
    		LOG.info("init the "+(++num)+" eventMap and headValue is:"+headValue);
    	}
    }

    /**
     * 服务关闭时执行
     */
    @Override
    public void stop() {
    	super.stop();
    	LOG.info("BaseAdminMysqlSink stop...");
    }
    
    /**获得连接*/
    protected Connection getConnection(String headValue){
    	Connection connection = null;
		try {
			connection = DpcpConnectionPoolManager.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	usingConnections.put(headValue, connection);
		return connection;
    }
    
    /**
     *  执行的事情：<br/>
          	（1）持续不断的从channel中获取event放到batchSize大小的数组中<br/>
          	（2）event可以获取到则进行event处理，否则返回Status.BACKOFF标识没有数据提交<br/>
          	（3）batchSize中有内容则进行jdbc提交<br/>
     */
    public Status process() throws EventDeliveryException {
    	isSuccessProcess.set(true);;//初始
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
        	/*event获取*/
            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event != null) {
                	String content = new String(event.getBody());
                	LOG.info("1.get event body:"+content);
                	this.eventMap.get(event.getHeaders().get(Constant.processorHeadKeyName)).add(event);
                } else {
                    result = Status.BACKOFF;
                    break;
                }
            }
            
            /*有几个processor将要执行*/
            int processorToDoCount = 0;
            for(List<Event> values :this.eventMap.values()){
            	if(values.size() > 0){
            		processorToDoCount++;
            	}
            }
            /*执行*/
            if(processorToDoCount > 0){
            	final CountDownLatch latch = new CountDownLatch(processorToDoCount);
            	for(final String headValue:ProcessorPoolManager.getProcessorSingleInstanceMap().keySet()){
            		if(eventMap.get(headValue).size() > 0){
            			scheduledExecutorService.execute(new Runnable() {
            				public void run() {
        						try {
        							AbstractEventProcessor abstractEventProcessor = ProcessorPoolManager.processorSingleInstanceMap.get(headValue);
									abstractEventProcessor.baseEventProcessor(eventMap.get(headValue), BaseAdminMysqlSink.this.getConnection(headValue));
								} catch (SQLException e) {
									e.printStackTrace();
									isSuccessProcess.set(false);;
								}
        						latch.countDown();
            				}
            			});
            		}
            	}
            	latch.await();
            }
//            transaction.commit();
            
            if(isSuccessProcess.get()){
            	this.transactionAndConnectionCommit(transaction);
            }else{
            	this.transactionAndConnectionRollBack(transaction);
            	System.out.println("processor exception!");
            }
        } catch (Exception e) {
            try {
            	this.transactionAndConnectionRollBack(transaction);
//                transaction.rollback();
            } catch (Exception e2) {
                LOG.error("Exception in rollback. Rollback might not have been.successful.", e2);
            }
            LOG.error("Failed to commit transaction.Transaction rolled back.", e);
            Throwables.propagate(e);
        } finally {
            transaction.close();
        }
        return result;
    }
    
    private void transactionAndConnectionCommit(Transaction transaction){
    	
    	System.out.println("--------------------------------------9.BaseTranscon:");
    	if(usingConnections.size() > 0){
    		System.out.println("--------------------------------------10.BaseTranscon:");
    		Iterator<String> iterator = this.usingConnections.keySet().iterator();
    		while(iterator.hasNext()){
    			String headValue = iterator.next();
    			try {
    				System.out.println("--------------------------------------11.BaseTranscon:");
    				if(!this.usingConnections.get(headValue).getAutoCommit()){
    					this.usingConnections.get(headValue).commit();
    					System.out.println("--------------------------------------12.BaseTranscon:");
    				}
    				//数据提交后，把存储的数据删除
//    				this.eventBodyMap.get(headValue).clear();
    				this.eventMap.get(headValue).clear();
    				System.out.println("--------------------------------------13.BaseTranscon:");
				} catch (SQLException e) {
					e.printStackTrace();
				}finally{
					Connection remove = this.usingConnections.remove(headValue);
					DpcpConnectionPoolManager.release(remove);
					System.out.println("--------------------------------------14.BaseTranscon:");
//					this.releaseConnection(remove);
				}
    		}
    	}
    	transaction.commit();
    	System.out.println("--------------------------------------15.BaseTranscon:");
    }
    
    private void transactionAndConnectionRollBack(Transaction transaction){
    	if(usingConnections.size() > 0){
    		Iterator<String> iterator = this.usingConnections.keySet().iterator();
    		while(iterator.hasNext()){
    			String headValue = iterator.next();
    			try {
    				if(!this.usingConnections.get(headValue).getAutoCommit()){
    					this.usingConnections.get(headValue).rollback();
    				}
				} catch (SQLException e) {
					e.printStackTrace();
				}finally{
					Connection remove = this.usingConnections.remove(headValue);
					DpcpConnectionPoolManager.release(remove);
//					this.releaseConnection(remove);
				}
    		}
    		
    		transaction.rollback();
    	}
    }
    
//    private void releaseConnection(Connection connection){
//    	this.connectionPool.add(connection);
//    }
    
    //暂时只有AccountChangeSink有使用
//  protected PreparedStatement getPreparedStatement(Connection conn, String tableName, Class clazz){
//  	if(clazz == null){
//  		return null;
//  	}
//  	
//  	Field[] fields = clazz.getDeclaredFields();
//  	StringBuffer propStringBuffer = new StringBuffer();
//  	StringBuffer _stringBuffer = new StringBuffer();
//  	for(Field filed:fields){
//  		filed.setAccessible(true);
//  		propStringBuffer.append(filed.getName()).append(",");
//  		_stringBuffer.append("?,");
//  	}
//  	
//  	String propString = propStringBuffer.substring(0, propStringBuffer.lastIndexOf(","));
//  	String _string = _stringBuffer.substring(0, _stringBuffer.lastIndexOf(","));
//  	PreparedStatement preparedStatement = null;
//		try {
//			preparedStatement = conn.prepareStatement("insert into " + tableName +
//					" ("+propString+") values ("+_string+")");
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//	
//  	return preparedStatement;
//  }
    
}


