package com.rw.persistence;

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;

public class RWObjectMgr {
	static final Logger log = Logger.getLogger(RWObjectMgr.class.getName());
	public RWJApplication app = null;
	public JedisMT jedisMt = new JedisMT();

	public BasicDBObject executionContext = null;

	// Default constructor.
	// Should setup the context immediately.
	public RWObjectMgr() {
		executionContext = new BasicDBObject();
		try {
			dataMT t = (dataMT)Thread.currentThread();
			setTenantKey(t.tenant.get());
		}
		catch(Exception e){
			setTenantKey(System.getProperty("PARAM3"));	
		}
		
		app = new RWJApplication(this);
	}
	
	// This constructor is useful only if the context can restored.
	public RWObjectMgr( String userid ) {
		if (!restoreExecutionContext(userid) ) {
			executionContext = new BasicDBObject();
			executionContext.put("userid", userid);		
		}
		app = new RWJApplication(this);
	}
	
	// This constructor is for propagating the context
	public RWObjectMgr(RWObjectMgr context) throws Exception {
		cloneExecutionContext(context);
		app = new RWJApplication(this);		
	}

	public void Cache(String token, String data) {
		jedisMt.set("Cache." + token, data);	
	}

	public String Cache(String token) {
		return jedisMt.get("Cache." + token);
	}

	public Long Decache(String token) {
		return jedisMt.del("Cache." + token);
	}
	
	public String StringifyDBObject(BasicDBObject obj) {
		log.trace( "StringifyDBObject");

		ObjectSerializer s = JSONSerializers.getStrict();
		String dataout = s.serialize(obj);
		return dataout;
	}

	public String StringifyDBList(BasicDBList obj) {
		log.trace( "StringifyDBList");

		ObjectSerializer s = JSONSerializers.getStrict();
		String dataout = s.serialize(obj);
		return dataout;
	}
	
	public void setExecutionContextItem(String name, Object value ) {
		executionContext.put(name, value);
		if ( name.equals("tenant"))
			jedisMt.setTenant(value.toString());
	}
	
	public void cloneExecutionContext(RWObjectMgr source) throws Exception {
		executionContext = source.getExecutionContext();
		jedisMt.setTenant(getTenantKey());
	}
	
	public Object getExecutionContextItem(String name) throws Exception {
		return executionContext.get(name);
	}
	
	public BasicDBObject getExecutionContext(){
		return executionContext;
	}
	
	public String getUserId() throws Exception {
		return executionContext.get("userid").toString();
	}
	
	public String getTenantKey() throws Exception {
		return executionContext.get("tenant") == null ? System.getProperty("PARAM3") : executionContext.get("tenant").toString();		
	}

	public void setTenantKey(String tenantStr) {
		executionContext.put("tenant", tenantStr);
		jedisMt.setTenant(tenantStr);
	}

	public void setUserId(String userid) {
			executionContext.put("userid", userid);		
	}

	public String GetPartyId() throws Exception {
		log.trace( "GetPartyId");		
		
		// See if the partyId is already in the execution context
		Object a = executionContext.get("partyId");
		String partyId = (a != null) ? a.toString() : null;
		if (partyId != null ) return partyId ;
		
		// See if the partyId is in the cache
		String userid = executionContext.get("userid").toString();		
		partyId = jedisMt.hget(userid , "partyId");
		
		// Get it from the db
		if (partyId == null ) {
			// PartyId Should be in the cache 99% of the time
			RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");

			BasicDBObject query = new BasicDBObject();

			query.put("credId", userid.toString());
			
			QueryBuilder qb = new QueryBuilder().or(new QueryBuilder().start().put("partytype").is("PARTNER").get(),
					new QueryBuilder().start().put("partytype").is("PARTNER_DEMO").get());
			query.putAll(qb.get());
			if (bc.ExecQuery(query,null) == 1 ) {
				partyId = bc.GetFieldValue("_id").toString();
				jedisMt.hset(userid, "partyId", partyId);
				// put it in the context
				executionContext.put("partyId", partyId);
			}
		}
		
		return partyId;
	}
	
	@SuppressWarnings("unchecked")
	public BasicDBObject GetParty(String party_id) throws Exception {
		log.trace( "GetParty");
		if ( party_id == null)
			party_id = GetPartyId();
			// This is redundant. 99% of the cases should be served by the top portion
			RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
			BasicDBObject query = new BasicDBObject();
			query.put("_id", new ObjectId(party_id));
			
			if (bc.ExecQuery(query,null) == 1 ) {
				return bc.GetCompositeRecord();
			}
		return null;
	}
	@SuppressWarnings("unchecked")
	public Object GetSelfAttr(String attr) throws Exception {
		log.trace( "GetSelfAttr");
		if ( attr == "userid" )
				return executionContext.get("userid").toString();
		else {
			return GetPartyAttr(null,attr);
		}
	}
	
	@SuppressWarnings("unchecked")
	public Object GetPartyAttr(String partyId, String attr) throws Exception {
		log.trace( "GetPartyAttr " + partyId + " " + attr);
		try {
			return GetParty(partyId).get(attr);
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			log.debug(e.getStackTrace());
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public String GetPartyIdFromUid(String uid) throws Exception {
		log.trace( "GetPartyId");
		String partyId = jedisMt.hget(uid, "partyId");
		if (partyId == null ) {
			RWJBusComp bc = GetPartyFromUid(uid);
			partyId = bc.GetFieldValue("_id").toString();
			jedisMt.hset(uid, "partyId", partyId);
		}
		return partyId;
	}

	@SuppressWarnings("unchecked")
	public RWJBusComp GetPartyFromUid(String uid) throws Exception {
		log.trace( "GetPartyId");
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();		
		query.put("credId", uid);
		query.put("partytype", "PARTNER");
		if (bc.ExecQuery(query,null) == 1 ) {
			return bc;
		}
		throw new Exception("Party Object Not found");
	}

	public JSONObject getTenant(JSONObject data) throws Exception {		
		String tenantKey = getTenantKey();
		BasicDBObject found = null;
		String tenantStr = Cache(tenantKey);
		if ( tenantStr == null ) {
			
			RWJBusComp bc = app.GetBusObject("Tenant").getBusComp("Tenant");
			
			BasicDBObject query = new BasicDBObject();
			query.put("tenant", tenantKey);
			
			int nRecs = bc.ExecQueryMaster(query);
			if ( nRecs == 0) {
				String errorMessage = "Tenant not found : " + tenantKey ;  
				log.debug( errorMessage);
				throw new Exception(errorMessage);
			}
			// put this in the data cache
			found = bc.currentRecord;
			Cache(tenantKey, StringifyDBObject(found));
		}
		else 
		{
			found = (BasicDBObject) JSON.parse(tenantStr);
		}		
		return new JSONObject(found.toMap());
	}
	
	public void saveExecutionContext() throws Exception {
		String userid = getExecutionContextItem("userid").toString();
		Cache("EC." + userid, StringifyDBObject(executionContext));
	}
	
	public boolean restoreExecutionContext(String userid) {
		if (userid != null) {
			String strParty = Cache("EC." + userid );
			if (strParty != null) {
				executionContext = (BasicDBObject) JSON.parse(strParty);
				jedisMt.setTenant(executionContext.getString("tenant"));
				return true;
			}
		}
		return false;
	}
	
	public void deleteExecutionContext() throws Exception {		
		String userid = getExecutionContextItem("userid").toString();
		Decache("EC." + userid);
	}
	
}
