package com.rw.persistence;

import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.lang.Math;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.rw.repository.ALGORITHM;

public class RWAlgorithm {
	static final Logger log = Logger.getLogger(RWAlgorithm.class.getName());
	
	private RWObjectMgr objMgr = null;
	
	public RWAlgorithm () {
	}
	
	public RWAlgorithm (RWObjectMgr context) throws Exception {
		  objMgr = context;
	}
	
	public double compute( BasicDBObject entity, String dqRule) throws Exception {

		mongoStore m = new mongoStore(objMgr.getTenantKey());
		BasicDBObject query = new BasicDBObject();
		query.put("name", dqRule);
		BasicDBObject reference = (BasicDBObject) m.getColl("rwDQRule").findOne(query);

		String alg = (String) reference.get("Algorithm");

		if ( alg.equals("completeness"))
			return completeness(entity, reference,ALGORITHM.SUM );
		else if ( alg.equals("similarity"))
			return similarity(entity, reference,ALGORITHM.SIMILARITY );
		else if ( alg.equals("polynomial"))
			return polynomial(entity, reference,ALGORITHM.SUM );
		else return 0L;	
	}
		
	public double completeness( BasicDBObject entity, BasicDBObject reference, ALGORITHM strategy) {
		
		double retVal = 0L;
		double expected = 0L;
		double score = 0L;
		
		BasicDBList defs = (BasicDBList) reference.get("definition");

		for ( int i = 0; i < defs.size(); i++) {
			BasicDBObject d = (BasicDBObject) defs.get(i);
			String key = d.getString("fldname");
			double val = d.getDouble("value");
			
			Object o = entity.get(key);
			
			if ( o != null && !o.toString().isEmpty() ) {
				retVal += val;
			}
			expected += val; 			
		}
		
		score = retVal/expected;
		return score;
		
	}
	
	public double similarity( BasicDBObject entity, BasicDBObject reference, ALGORITHM strategy) {
		
		double retVal = 0L;
		double  expected = 0L;
		double score = 0L;
		Set<String> keys = reference.keySet();
		
		for ( String key : keys ) {
			Object o1 = entity.get(key);
			if ( o1 != null && !o1.toString().isEmpty() ) {
				retVal += 1;
				Object o2 = reference.get(key);
				if ( o2 != null && !o2.toString().isEmpty() ) {
					expected += 1;
				}
			}
		}
		
		score = retVal/expected;
		return score;
		
	}
	
	public double polynomial( BasicDBObject entity, BasicDBObject reference, ALGORITHM strategy) {		
		double retVal = 0L;
		BasicDBList defs = (BasicDBList) reference.get("definition");
		for ( int i = 0; i < defs.size(); i++) {
			BasicDBObject d = (BasicDBObject) defs.get(i);
			String key = d.getString("fldname");
			double multiplier = d.getDouble("multiplier");
			int power = d.getInt("power");
			double l = 0L;
			if ( entity.containsField(key)) 
				l = entity.getDouble(key);			
			if ( l > 0 ) {
				retVal += Math.pow( l, (double)power) * multiplier;
			}		
		}		
		return retVal;		
	}
	
}
