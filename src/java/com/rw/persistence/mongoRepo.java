package com.rw.persistence;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.ResourceBundle;

// import net.sf.json.JSONObject;
//import net.sf.json.xml.XMLSerializer;









import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.*;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class mongoRepo  implements database {
	static final Logger log = Logger.getLogger(mongoRepo.class.getName());

	static Mongo m = null;
	final static String connection = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("connection");
	String tenant = null;
	
	private JedisMT jedisMt = new JedisMT(); 
	
	public mongoRepo() {
		try {
			dataMT t = (dataMT)Thread.currentThread();
			tenant = t.tenant.get();
		}
		catch(Exception e){
			tenant = System.getProperty("PARAM3");	
		}		
	}
	
	public mongoRepo(String tenantStr) throws Exception {
		tenant = tenantStr;
		jedisMt.setTenant(tenantStr);
	}
	
	
	public DB getDB() {
		
		try {
			if ( m == null ) {
				m = new MongoClient( System.getProperty("JDBC_CONNECTION_STRING") == null ? connection : System.getProperty("JDBC_CONNECTION_STRING"));
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);

		}
		return m.getDB( tenant + "repo") ;
	}
	
	public DBCollection getColl(String col)
	{
		return getDB().getCollection(col);
	}
	
	
	public org.json.JSONObject GetBCByName(String name) {
		log.trace( "GetBCByName() : " + name  );
		DBObject found = null;
		String cachedValue = getCache("bc_" + name);
		
		if (cachedValue != null)
			found = (DBObject) JSON.parse(cachedValue);
		else {
			BasicDBObject query = new BasicDBObject();
			query.put("name", name);
			found = getColl("BusComp").findOne(query);
			putCache("bc_" + name,JSON.serialize(found));
		}
		
		org.json.JSONObject jso = new org.json.JSONObject(found.toMap());
		return jso;
	}
	
	public org.json.JSONObject GetBOByName(String name) {
		log.trace( "GetBOByName() : " + name );

		DBObject found = null;
		String cachedValue = getCache("bo_" + name);
		
		if (cachedValue != null)
			found = (DBObject) JSON.parse(cachedValue);
		else {
			BasicDBObject query = new BasicDBObject();
			query.put("name", name);
			found = getColl("BusObj").findOne(query);
			putCache("bo_" + name,JSON.serialize(found));
		}
		
		org.json.JSONObject jso = new org.json.JSONObject(found.toMap());
		return jso;
	}
	
	public org.json.JSONObject GetAppletByName(String name) {
		log.trace( "GetAppletByName() :" + name );
		System.out.println( "GetAppletByName() :" + name );
		DBObject found = null;
		String cachedValue = getCache("MetaApplet_" + name);

		if (cachedValue != null)
			found = (DBObject) JSON.parse(cachedValue);
		else {
			BasicDBObject query = new BasicDBObject();
			query.put("name", name);
			found = getColl("Applet").findOne(query);
			String s = found.get("_id").toString();
	        found.put("_id",s);
			putCache("MetaApplet_" + name,JSON.serialize(found));
		}
		org.json.JSONObject jso = new org.json.JSONObject(found.toMap());
		return jso;
	}
	
	public org.json.JSONObject GetChartAppletByName(String name) {
		log.trace( "GetChartAppletByName() :" + name );
		System.out.println( "GetChartAppletByName() :" + name );

		DBObject found = null;
		String cachedValue = getCache("ChartApplet_" + name);

		if (cachedValue != null)
			found = (DBObject) JSON.parse(cachedValue);
		else {
			BasicDBObject query = new BasicDBObject();
			query.put("name", name);
			found = getColl("ChartApplet").findOne(query);
			String s = found.get("_id").toString();
	        found.put("_id",s);
			putCache("ChartApplet_" + name,JSON.serialize(found));
		}
		org.json.JSONObject jso = new org.json.JSONObject(found.toMap());
		return jso;
	}
	public org.json.JSONObject GetChartDataByName(String name) {
		log.trace( "GetChartDataByName() :" + name );
		System.out.println( "GetChartDataByName() :" + name );

		DBObject found = null;
		String cachedValue = getCache("ChartApplet_" + name);

		if (cachedValue != null)
			found = (DBObject) JSON.parse(cachedValue);
		else {
			BasicDBObject query = new BasicDBObject();
			query.put("name", name);
			found = getColl("ChartData").findOne(query);
			String s = found.get("_id").toString();
	        found.put("_id",s);
			putCache("ChartData_" + name,JSON.serialize(found));
		}
		org.json.JSONObject jso = new org.json.JSONObject(found.toMap());
		return jso;
	}
	public void cleanUp() {
		
		String[] collections = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("repoTables").split(",");
		for (int i = 0; i < collections.length; i++)
			getColl(collections[i]).drop();
	}

	public void loadObjects(JSONObject json, String metaCollName, String mongoCollName ) throws JSONException {
	
	    if ( json.isNull(metaCollName) ) return; 	

		Object coll = (Object) json.get(metaCollName);
	    if ( coll instanceof JSONArray ) {
			  JSONArray defs = (JSONArray ) coll;
			  for (int i =0; i < defs.length(); i++) {
				  Object obj = (JSONObject) defs.get(i);
				  getColl(mongoCollName).insert( (DBObject) com.mongodb.util.JSON.parse(obj.toString()) );
				  delCache(metaCollName + "_" + mongoCollName );
				  log.trace( obj.toString() );
			  }
		  }
		  else {
			  getColl(mongoCollName).insert( (DBObject) com.mongodb.util.JSON.parse(coll.toString()) );
			  log.trace( coll.toString() );
		  }
	}

	public void updateObjects(JSONObject json, String metaCollName, String mongoCollName ) throws JSONException {
		
	    if ( json.isNull(metaCollName) ) return; 	

		Object coll = (Object) json.get(metaCollName);
	    if ( coll instanceof JSONArray ) {
			  JSONArray defs = (JSONArray ) coll;
			  for (int i =0; i < defs.length(); i++) {
				  JSONObject obj = (JSONObject) defs.get(i);
				  BasicDBObject o = (BasicDBObject) com.mongodb.util.JSON.parse(obj.toString());
				  BasicDBObject q = new BasicDBObject();
				  q.put("name", obj.get("name"));
				  getColl(mongoCollName).update(q, o, true, false);
				  delCache(metaCollName + "_" + mongoCollName );
				  log.trace( obj.toString() );
			  }
		  }
		  else {
			  BasicDBObject o = (BasicDBObject) com.mongodb.util.JSON.parse(coll.toString());
			  BasicDBObject q = new BasicDBObject();
			  q.put("name", o.get("name"));
			  getColl(mongoCollName).update(q, o, true, false);
			  delCache(metaCollName + "_" + mongoCollName );
			  log.trace( coll.toString() );
		  }
	}

	public void LoadMetaData(String metadatafile) throws JSONException
	{
		try {
		      InputStream is = this.getClass().getResourceAsStream(metadatafile);
		      String xml = IOUtils.toString(is);
		 	  JSONObject rfMeta = XML.toJSONObject(xml);
		      if ( !rfMeta.isNull("ReferralWireMetaData") ) { 	
		 	  	 JSONObject json = rfMeta.getJSONObject("ReferralWireMetaData");
		    	 loadObjects(json, "bc", "BusComp") ;
		    	 loadObjects(json, "bo", "BusObj") ;
		    	 loadObjects(json, "MetaApplet", "Applet") ;
		    	 loadObjects(json, "ChartData", "ChartData");
		    	 loadObjects(json, "ChartApplet", "ChartApplet") ;
		      }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("Error loading : "  + metadatafile);
			log.debug(e.getStackTrace());
		}
	}

	public void UpdateMetaData(FileReader f) throws JSONException
	{
		try {
		      String xml = IOUtils.toString(f);
		 	  JSONObject rfMeta = XML.toJSONObject(xml);
		      if ( !rfMeta.isNull("ReferralWireMetaData") ) { 	
		 	  	 JSONObject json = rfMeta.getJSONObject("ReferralWireMetaData");
		    	 updateObjects(json, "bc", "BusComp") ;
		    	 updateObjects(json, "bo", "BusObj") ;
		    	 updateObjects(json, "MetaApplet", "Applet") ;
		    	 updateObjects(json, "ChartData", "ChartData");
		    	 updateObjects(json, "ChartApplet", "ChartApplet") ;
		      }
		} catch (IOException e) {
			log.debug(e.getStackTrace());
		}
	}

	public void putCache(String token, String data) {
			jedisMt.set("METADATA_" + token, data);	
	}

	public String getCache(String token) {
			return jedisMt.get("METADATA_" + token);
	}
	public long delCache(String token) {
			return jedisMt.del("METADATA_" + token);
	}

	public void beginTrans() throws Exception {
		getDB().requestStart();
	}

	public void endTrans() throws Exception {
		getDB().requestDone();
	}

	public void requestEnsureConnection() throws Exception {
		getDB().requestEnsureConnection();
	}
}
