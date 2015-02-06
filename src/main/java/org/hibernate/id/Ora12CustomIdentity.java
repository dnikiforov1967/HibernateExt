package org.hibernate.id;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import oracle.jdbc.OraclePreparedStatement;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.id.SequenceIdentityGenerator.NoCommentsInsert;
import org.hibernate.id.insert.AbstractReturningDelegate;
import org.hibernate.id.insert.IdentifierGeneratingInsert;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.hibernate.dialect.Oracle12CustomDialect;

/**
 * This class provides 12c Identity generation.
 * @author Dmitry Nikifrov
 */

public class Ora12CustomIdentity extends AbstractPostInsertGenerator {

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
               "trigger assigned identity generator cannot be used with multi-column keys");
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

          insertSQL = insertSQL + ((Oracle12CustomDialect)dialect).getReturningClause(keyColumns[0]);
          System.out.println(insertSQL);
          OraclePreparedStatement os = (OraclePreparedStatement)session.connection().prepareStatement(insertSQL);
          keyId = insertSQL.split("\\?").length;
          os.registerReturnParameter(keyId, Types.DECIMAL);
          return os;
          
      }

      /**
       * {@inheritDoc}
       */
      @Override
      protected Serializable executeAndExtract(PreparedStatement insert, SessionImplementor session)
         throws SQLException {
          
        OraclePreparedStatement os = (OraclePreparedStatement)insert;
        os.executeUpdate();
      
        ResultSet generatedKeys = os.getReturnResultSet();
        if (generatedKeys == null) {
            throw new HibernateException("Nullable Resultset");
        }
        try {
           return IdentifierGeneratorHelper.getGeneratedIdentity(
                   generatedKeys, 
                   keyColumns[0],
                   getPersister().getIdentifierType());
        } finally {
            generatedKeys.close();
        }
      }
   }
}
