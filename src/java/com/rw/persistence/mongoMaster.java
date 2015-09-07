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

public class mongoMaster implements database {
	static final Logger log = Logger.getLogger(mongoMaster.class.getName());

	static Mongo m = null;
	final static String connection = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("connection");
	final static String dataStore = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("master");

	public mongoMaster() {}
	
	public DB getDB() {
		try {
			if ( m == null ) {
				m = new MongoClient( System.getProperty("JDBC_CONNECTION_STRING") == null ? connection : System.getProperty("JDBC_CONNECTION_STRING"));
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			log.debug("API Error: ", e);
		}
		return m.getDB(dataStore) ;
	}
	
	public void cleanUp() {
		String[] collections = ResourceBundle.getBundle("mongo", new Locale("en", "US")).getString("masterTables").split(",");
		for (int i = 0; i < collections.length; i++)
			getColl(collections[i]).drop();
		
	}
	
	public DBCollection getColl(String col)
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

	public boolean SaveRecord(String coll, org.json.JSONObject doc) {
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
