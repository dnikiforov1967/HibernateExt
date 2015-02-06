package org.hibernate.dialect;

import org.hibernate.dialect.Oracle10gDialect;

/**
 * This class provides few additional functionality for standard Oracle10gDialect of Hibernate.
 * @author Dmitry Nikifrov
 */

public class Oracle12CustomDialect extends Oracle10gDialect {
	
	/**
	*
	* This method provides RETURNING clause
	*
	* @param idColumnName
	*/
	public String getReturningClause(String idColumnName) {
		return " returning " + idColumnName + " into ?";
	}
	
}