package com.rw.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Locale;
import java.util.ResourceBundle;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

public class mongoStore implements database {
	static final Logger log = Logger.getLogger(mongoStore.class.getName());

	static Mongo m = null;
	final static String connection = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("connection");
	String tenant = null;
	
	public mongoStore() {
		try {
			dataMT t = (dataMT)Thread.currentThread();
			tenant = t.tenant.get();
		}
		catch(Exception e){
			tenant = System.getProperty("PARAM3");	
		}		
	}
	
	public mongoStore(String tenantStr) throws Exception {
		tenant = tenantStr;
		if ( tenant == null ) throw new Exception("tenant is null");

	}
	
	public void setTenant(String tenantStr) {
		tenant = tenantStr;		
	}
	
	public DB getDB() throws Exception {
		try {
			if ( m == null ) {
				m = new MongoClient( System.getProperty("JDBC_CONNECTION_STRING") == null ? connection : System.getProperty("JDBC_CONNECTION_STRING"));
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
		return m.getDB( tenant + "data") ;
	}
	
	public void cleanUp() throws Exception {
		String[] collections = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("dataTables").split(",");
		for (int i = 0; i < collections.length; i++)
			getColl(collections[i]).drop();
	}
	
	public DBCollection getColl(String col) throws Exception
	{
		log.trace( "getColl() : " + col  );

		return getDB().getCollection(col);
	}
	
	public org.json.JSONObject NewRecord(org.json.JSONObject buscomp) throws JSONException {
		org.json.JSONObject jso = new org.json.JSONObject();

		BasicDBList columns = (BasicDBList) buscomp.get("field");
		
		for (int i=0; i < columns.size(); i++) {
			BasicDBObject col = (BasicDBObject) columns.get(i);
			jso.put(col.get("fldname").toString(), "");
		}
		
		return jso;
	}

	public boolean SaveRecord(String coll, org.json.JSONObject doc) throws Exception {
		getColl(coll).save( (DBObject) com.mongodb.util.JSON.parse(doc.toString()) );
		return true;
	}
	
	public boolean SetAttribute(org.json.JSONObject doc, String fldname, Object value) throws JSONException {
		doc.put(fldname, value.toString());
		return true;
	}

	public Object GetAttribute(org.json.JSONObject doc, String fldname) throws JSONException {
		return doc.get(fldname);
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
