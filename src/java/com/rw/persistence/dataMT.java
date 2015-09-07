package com.rw.persistence;

public class dataMT extends Thread {
	
	public final ThreadLocal<String> tenant = new ThreadLocal<String>() {
		protected String initialValue() {
		//	return "RW";
			return "STN";
		}
	};
	public dataMT(String tenantStr) {
		tenant.set(tenantStr);
	}
	@Override
	public void run()
	{
	}
}
