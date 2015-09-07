package com.rw.persistence;

public class RWJApplication {
	
	private RWObjectMgr objMgr = null;
	
	public RWJApplication () {}
	public RWJApplication (RWObjectMgr context) { objMgr = context;}
	
	public RWJBusObj GetBusObject(String BOName) throws Exception {
		return new RWJBusObj(this, BOName);
	}
	
	public RWObjectMgr getContext() {
		return objMgr;
	}

	public void setContext(RWObjectMgr omgr) {
		objMgr = omgr;
	}
	
	public RWChartItemData GetChartObject(String name) throws Exception {
		return new RWChartItemData(name, objMgr);
	}
	
	public RWChartItemData GetChartObject() throws Exception {
		return new RWChartItemData(objMgr);
	}
	
}
