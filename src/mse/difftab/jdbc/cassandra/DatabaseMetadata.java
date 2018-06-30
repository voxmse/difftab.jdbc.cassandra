/**
 * 
 */
package mse.difftab.jdbc.cassandra;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class DatabaseMetadata implements DatabaseMetaData {
	
	/* (non-Javadoc)
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#allProceduresAreCallable()
	 */
	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#allTablesAreSelectable()
	 */
	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#autoCommitFailureClosesAllResultSets()
	 */
	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#dataDefinitionCausesTransactionCommit()
	 */
	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#dataDefinitionIgnoredInTransactions()
	 */
	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#deletesAreDetected(int)
	 */
	@Override
	public boolean deletesAreDetected(int arg0) throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#doesMaxRowSizeIncludeBlobs()
	 */
	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#generatedKeyAlwaysReturned()
	 */
	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		return false;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getAttributes(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getAttributes(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getBestRowIdentifier(java.lang.String, java.lang.String, java.lang.String, int, boolean)
	 */
	@Override
	public ResultSet getBestRowIdentifier(String arg0, String arg1, String arg2, int arg3, boolean arg4) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getCatalogSeparator()
	 */
	@Override
	public String getCatalogSeparator() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getCatalogTerm()
	 */
	@Override
	public String getCatalogTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getCatalogs()
	 */
	@Override
	public ResultSet getCatalogs() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getClientInfoProperties()
	 */
	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getColumnPrivileges(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getColumnPrivileges(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getCrossReference(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getCrossReference(String arg0, String arg1, String arg2, String arg3, String arg4, String arg5)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDatabaseMajorVersion()
	 */
	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDatabaseMinorVersion()
	 */
	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDatabaseProductName()
	 */
	@Override
	public String getDatabaseProductName() throws SQLException {
		return "Cassandra";
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDatabaseProductVersion()
	 */
	@Override
	public String getDatabaseProductVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDefaultTransactionIsolation()
	 */
	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDriverMajorVersion()
	 */
	@Override
	public int getDriverMajorVersion() {
		return Driver.MAJOR_VERSION;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDriverMinorVersion()
	 */
	@Override
	public int getDriverMinorVersion() {
		return Driver.MINOR_VERSION;
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDriverName()
	 */
	@Override
	public String getDriverName() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getDriverVersion()
	 */
	@Override
	public String getDriverVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getExportedKeys(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getExportedKeys(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getExtraNameCharacters()
	 */
	@Override
	public String getExtraNameCharacters() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getFunctionColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getFunctionColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getFunctions(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getFunctions(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getIdentifierQuoteString()
	 */
	@Override
	public String getIdentifierQuoteString() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getImportedKeys(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getImportedKeys(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getIndexInfo(java.lang.String, java.lang.String, java.lang.String, boolean, boolean)
	 */
	@Override
	public ResultSet getIndexInfo(String arg0, String arg1, String arg2, boolean arg3, boolean arg4)
			throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getJDBCMajorVersion()
	 */
	@Override
	public int getJDBCMajorVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getJDBCMinorVersion()
	 */
	@Override
	public int getJDBCMinorVersion() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxBinaryLiteralLength()
	 */
	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxCatalogNameLength()
	 */
	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxCharLiteralLength()
	 */
	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnNameLength()
	 */
	@Override
	public int getMaxColumnNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnsInGroupBy()
	 */
	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnsInIndex()
	 */
	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnsInOrderBy()
	 */
	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnsInSelect()
	 */
	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxColumnsInTable()
	 */
	@Override
	public int getMaxColumnsInTable() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxConnections()
	 */
	@Override
	public int getMaxConnections() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxCursorNameLength()
	 */
	@Override
	public int getMaxCursorNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxIndexLength()
	 */
	@Override
	public int getMaxIndexLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxProcedureNameLength()
	 */
	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxRowSize()
	 */
	@Override
	public int getMaxRowSize() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxSchemaNameLength()
	 */
	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxStatementLength()
	 */
	@Override
	public int getMaxStatementLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxStatements()
	 */
	@Override
	public int getMaxStatements() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxTableNameLength()
	 */
	@Override
	public int getMaxTableNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxTablesInSelect()
	 */
	@Override
	public int getMaxTablesInSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getMaxUserNameLength()
	 */
	@Override
	public int getMaxUserNameLength() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getNumericFunctions()
	 */
	@Override
	public String getNumericFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getPrimaryKeys(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getPrimaryKeys(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getProcedureColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getProcedureColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getProcedureTerm()
	 */
	@Override
	public String getProcedureTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getProcedures(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getProcedures(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getPseudoColumns(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getPseudoColumns(String arg0, String arg1, String arg2, String arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getResultSetHoldability()
	 */
	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getRowIdLifetime()
	 */
	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSQLKeywords()
	 */
	@Override
	public String getSQLKeywords() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSQLStateType()
	 */
	@Override
	public int getSQLStateType() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSchemaTerm()
	 */
	@Override
	public String getSchemaTerm() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSchemas()
	 */
	@Override
	public ResultSet getSchemas() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSchemas(java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getSchemas(String arg0, String arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSearchStringEscape()
	 */
	@Override
	public String getSearchStringEscape() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getStringFunctions()
	 */
	@Override
	public String getStringFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSuperTables(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getSuperTables(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSuperTypes(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getSuperTypes(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getSystemFunctions()
	 */
	@Override
	public String getSystemFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getTablePrivileges(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getTablePrivileges(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getTableTypes()
	 */
	@Override
	public ResultSet getTableTypes() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getTables(java.lang.String, java.lang.String, java.lang.String, java.lang.String[])
	 */
	@Override
	public ResultSet getTables(String arg0, String arg1, String arg2, String[] arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getTimeDateFunctions()
	 */
	@Override
	public String getTimeDateFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getTypeInfo()
	 */
	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getUDTs(java.lang.String, java.lang.String, java.lang.String, int[])
	 */
	@Override
	public ResultSet getUDTs(String arg0, String arg1, String arg2, int[] arg3) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getURL()
	 */
	@Override
	public String getURL() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getUserName()
	 */
	@Override
	public String getUserName() throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#getVersionColumns(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public ResultSet getVersionColumns(String arg0, String arg1, String arg2) throws SQLException {
		throw new SQLFeatureNotSupportedException();

	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#insertsAreDetected(int)
	 */
	@Override
	public boolean insertsAreDetected(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#isCatalogAtStart()
	 */
	@Override
	public boolean isCatalogAtStart() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#isReadOnly()
	 */
	@Override
	public boolean isReadOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#locatorsUpdateCopy()
	 */
	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#nullPlusNonNullIsNull()
	 */
	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#nullsAreSortedAtEnd()
	 */
	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#nullsAreSortedAtStart()
	 */
	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#nullsAreSortedHigh()
	 */
	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#nullsAreSortedLow()
	 */
	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#othersDeletesAreVisible(int)
	 */
	@Override
	public boolean othersDeletesAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#othersInsertsAreVisible(int)
	 */
	@Override
	public boolean othersInsertsAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#othersUpdatesAreVisible(int)
	 */
	@Override
	public boolean othersUpdatesAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#ownDeletesAreVisible(int)
	 */
	@Override
	public boolean ownDeletesAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#ownInsertsAreVisible(int)
	 */
	@Override
	public boolean ownInsertsAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#ownUpdatesAreVisible(int)
	 */
	@Override
	public boolean ownUpdatesAreVisible(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesLowerCaseIdentifiers()
	 */
	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesLowerCaseQuotedIdentifiers()
	 */
	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesMixedCaseIdentifiers()
	 */
	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesMixedCaseQuotedIdentifiers()
	 */
	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesUpperCaseIdentifiers()
	 */
	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#storesUpperCaseQuotedIdentifiers()
	 */
	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsANSI92EntryLevelSQL()
	 */
	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsANSI92FullSQL()
	 */
	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsANSI92IntermediateSQL()
	 */
	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsAlterTableWithAddColumn()
	 */
	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsAlterTableWithDropColumn()
	 */
	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
	 */
	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCatalogsInDataManipulation()
	 */
	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCatalogsInIndexDefinitions()
	 */
	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCatalogsInPrivilegeDefinitions()
	 */
	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCatalogsInProcedureCalls()
	 */
	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCatalogsInTableDefinitions()
	 */
	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsColumnAliasing()
	 */
	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsConvert()
	 */
	@Override
	public boolean supportsConvert() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsConvert(int, int)
	 */
	@Override
	public boolean supportsConvert(int arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCoreSQLGrammar()
	 */
	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsCorrelatedSubqueries()
	 */
	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsDataDefinitionAndDataManipulationTransactions()
	 */
	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsDataManipulationTransactionsOnly()
	 */
	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsDifferentTableCorrelationNames()
	 */
	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsExpressionsInOrderBy()
	 */
	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsExtendedSQLGrammar()
	 */
	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsFullOuterJoins()
	 */
	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsGetGeneratedKeys()
	 */
	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsGroupBy()
	 */
	@Override
	public boolean supportsGroupBy() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsGroupByBeyondSelect()
	 */
	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsGroupByUnrelated()
	 */
	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsIntegrityEnhancementFacility()
	 */
	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsLikeEscapeClause()
	 */
	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsLimitedOuterJoins()
	 */
	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMinimumSQLGrammar()
	 */
	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMixedCaseIdentifiers()
	 */
	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMixedCaseQuotedIdentifiers()
	 */
	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMultipleOpenResults()
	 */
	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMultipleResultSets()
	 */
	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsMultipleTransactions()
	 */
	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsNamedParameters()
	 */
	@Override
	public boolean supportsNamedParameters() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsNonNullableColumns()
	 */
	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossCommit()
	 */
	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOpenCursorsAcrossRollback()
	 */
	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossCommit()
	 */
	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOpenStatementsAcrossRollback()
	 */
	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOrderByUnrelated()
	 */
	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsOuterJoins()
	 */
	@Override
	public boolean supportsOuterJoins() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsPositionedDelete()
	 */
	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsPositionedUpdate()
	 */
	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsResultSetConcurrency(int, int)
	 */
	@Override
	public boolean supportsResultSetConcurrency(int arg0, int arg1) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsResultSetHoldability(int)
	 */
	@Override
	public boolean supportsResultSetHoldability(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsResultSetType(int)
	 */
	@Override
	public boolean supportsResultSetType(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSavepoints()
	 */
	@Override
	public boolean supportsSavepoints() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSchemasInDataManipulation()
	 */
	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSchemasInIndexDefinitions()
	 */
	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSchemasInPrivilegeDefinitions()
	 */
	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSchemasInProcedureCalls()
	 */
	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSchemasInTableDefinitions()
	 */
	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSelectForUpdate()
	 */
	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsStatementPooling()
	 */
	@Override
	public boolean supportsStatementPooling() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsStoredFunctionsUsingCallSyntax()
	 */
	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsStoredProcedures()
	 */
	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSubqueriesInComparisons()
	 */
	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSubqueriesInExists()
	 */
	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSubqueriesInIns()
	 */
	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsSubqueriesInQuantifieds()
	 */
	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsTableCorrelationNames()
	 */
	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsTransactionIsolationLevel(int)
	 */
	@Override
	public boolean supportsTransactionIsolationLevel(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsTransactions()
	 */
	@Override
	public boolean supportsTransactions() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsUnion()
	 */
	@Override
	public boolean supportsUnion() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#supportsUnionAll()
	 */
	@Override
	public boolean supportsUnionAll() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#updatesAreDetected(int)
	 */
	@Override
	public boolean updatesAreDetected(int arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#usesLocalFilePerTable()
	 */
	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	/* (non-Javadoc)
	 * @see java.sql.DatabaseMetaData#usesLocalFiles()
	 */
	@Override
	public boolean usesLocalFiles() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
