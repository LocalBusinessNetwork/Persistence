package com.rw.persistence;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class RWJBusObj {

  static final Logger log = Logger.getLogger(RWJBusObj.class.getName());
	
  private RWJApplication app;
  private String name = null;
  private DBObject metadata = null;
 
  public RWJBusObj(RWJApplication a, String pname) throws Exception { 
	  
	  app = a; 
	  name = new String(pname);
	  
	  BasicDBObject query = new BasicDBObject();
	  query.put("name", name);
	  mongoRepo m = new mongoRepo(a.getContext().getTenantKey());
	  metadata = m.getColl("BusObj").findOne(query);
  };
  
  public RWJApplication getApp() { return app; }
  
  public RWJBusComp getBusComp(String bcName) throws Exception{
	  try {
		  BasicDBList bcs = (BasicDBList) metadata.get("bc");
		  
		  for (int i= 0; i < bcs.size(); i++) {
			  BasicDBObject bc = (BasicDBObject) bcs.get(i);
			  if ( bc.get("name").toString().equals(bcName) )
				  return new RWJBusComp(bcName, this);
		  }
	  }
	  catch (ClassCastException e) {
		  
		  BasicDBObject bc = (BasicDBObject) metadata.get("bc");
		  if (bc != null && bc.get("name").toString().equals(bcName)) {
			  return new RWJBusComp(bcName, this);
		  }
		  
	  }
	  return null;
  }  
  
}
