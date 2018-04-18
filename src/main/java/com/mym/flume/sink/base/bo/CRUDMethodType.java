package com.mym.flume.sink.base.bo;

/**
 * 数据库基本操作的方法
 * @author mym
 *
 */
public enum CRUDMethodType {
	
	insert("insert"),
	
	update("update"),//根据primaryKey更新
	
	;
	
	String name;
	
	CRUDMethodType(String name) {
		this.name = name;
	}
	
	public static CRUDMethodType getByName(String name){
		for(CRUDMethodType crud:CRUDMethodType.values()){
			if(crud.name.equalsIgnoreCase(name)){
				return crud;
			}
		}
		
		return null;
	}

}
