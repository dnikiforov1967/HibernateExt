package org.hibernate.id;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.CallableStatement;
import java.math.BigInteger;
import java.math.BigDecimal;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.id.SequenceIdentityGenerator.NoCommentsInsert;
import org.hibernate.id.insert.AbstractReturningDelegate;
import org.hibernate.id.insert.IdentifierGeneratingInsert;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.dialect.Oracle12CustomDialect;
import org.hibernate.type.Type;

/**
 * This class provides 12c Identity generation. This class does not use any Oracle-specific features, just CallavleStatement
 * @author Dmitry Nikifrov
 */

public class OraCustomIdentity extends AbstractPostInsertGenerator {

   /**
    * {@inheritDoc}
    */
   @Override
   public InsertGeneratedIdentifierDelegate getInsertGeneratedIdentifierDelegate(
      PostInsertIdentityPersister persister, Dialect dialect, boolean isGetGeneratedKeysEnabled)
      throws HibernateException {
      if (dialect.getClass().equals(Oracle12CustomDialect.class)) 
        return new Delegate(persister, dialect);
      else
        throw new HibernateException("This class works only with Oracle12CustomDialect");  
   }

   public static class Delegate extends AbstractReturningDelegate {

      private Dialect dialect;
      private String[] keyColumns;
      private int keyId;

      public Delegate(PostInsertIdentityPersister persister, Dialect dialect) {
         super(persister);
         this.dialect = dialect;
         this.keyColumns = getPersister().getRootTableKeyColumnNames();
         if (keyColumns.length > 1) {
            throw new HibernateException(
               "Identity generator cannot be used with multi-column keys");
         }
      }

      /**
       * {@inheritDoc}
       */
      @Override
      public IdentifierGeneratingInsert prepareIdentifierGeneratingInsert() {
         return new NoCommentsInsert(dialect);
      }

      /**
       * {@inheritDoc}
       */
      @Override
      protected PreparedStatement prepare(String insertSQL, SessionImplementor session) 
              throws SQLException {

          insertSQL = "begin "+insertSQL + ((Oracle12CustomDialect)dialect).getReturningClause(keyColumns[0])+"; end;";
          System.out.println(insertSQL);
          CallableStatement cs = session.connection().prepareCall(insertSQL);
          keyId = insertSQL.split("\\?").length-1;
          cs.registerOutParameter(keyId, Types.VARCHAR);
          return cs;
          
      }

      /**
       * {@inheritDoc}
       */
      @Override
      protected Serializable executeAndExtract(PreparedStatement insert, SessionImplementor session)
         throws SQLException {
          
        CallableStatement cs = (CallableStatement)insert;
        cs.executeUpdate();
		String id = cs.getString(keyId);
		return get(id, getPersister().getIdentifierType());
      }
	  
	  private Serializable get(String value, Type type) {
			Class clazz = type.getReturnedClass();
		
			if ( clazz == Long.class ) {
				return new Long( value );
			}
			else if ( clazz == Integer.class ) {
				return new Integer( value );
			}
			else if ( clazz == Short.class ) {
				return new Short( value );
			}
			else if ( clazz == String.class ) {
				return value;
			}
			else if ( clazz == BigInteger.class ) {
				return new BigInteger( value );
			}
			else if ( clazz == BigDecimal.class ) {
				return new BigDecimal( value );
			}
			else {
				throw new IdentifierGenerationException(
						"Unrecognized id type : " + type.getName() + " -> " + clazz.getName()
				);
			}

	  }
	  
	  
   }
}