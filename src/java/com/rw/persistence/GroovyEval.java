package com.rw.persistence;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

public class GroovyEval {

	Object eval(String poly, Object entity) {
		Binding binding = new Binding();
		binding.setVariable("entity", entity);
		GroovyShell shell = new GroovyShell(binding);
		
		// TODO: Load polynomial definition
		
		String pdef = getPolynomialDefintion(poly); 
//		Object value = shell.evaluate("x = 123; return entity.get("fieldName") * x");
		Object val = shell.evaluate(pdef);
		return val;
	}

	private String getPolynomialDefintion(String poly) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
