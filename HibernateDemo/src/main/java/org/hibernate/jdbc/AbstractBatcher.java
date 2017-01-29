package org.hibernate.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import org.hibernate.AssertionFailure;
import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.ScrollMode;
import org.hibernate.TransactionException;
import org.hibernate.cfg.Settings;
import org.hibernate.connection.ConnectionProvider;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.SessionFactoryImplementor;
import org.hibernate.exception.JDBCExceptionHelper;
import org.hibernate.jdbc.util.FormatStyle;
import org.hibernate.jdbc.util.SQLStatementLogger;
import org.hibernate.stat.Statistics;
import org.hibernate.stat.StatisticsImplementor;
import org.hibernate.util.JDBCExceptionReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBatcher implements Batcher {
	private static int globalOpenPreparedStatementCount;
	private static int globalOpenResultSetCount;
	private int openPreparedStatementCount;
	private int openResultSetCount;
	protected static final Logger log = LoggerFactory.getLogger(AbstractBatcher.class);
	private final ConnectionManager connectionManager;
	private final SessionFactoryImplementor factory;
	private PreparedStatement batchUpdate;
	private String batchUpdateSQL;
	private HashSet statementsToClose = new HashSet();
	private HashSet resultSetsToClose = new HashSet();
	private PreparedStatement lastQuery;
	private boolean releasing = false;
	private final Interceptor interceptor;
	private long transactionTimeout = -1L;
	boolean isTransactionTimeoutSet;

	public AbstractBatcher(ConnectionManager connectionManager, Interceptor interceptor) {
		this.connectionManager = connectionManager;
		this.interceptor = interceptor;
		this.factory = connectionManager.getFactory();
	}

	public void setTransactionTimeout(int seconds) {
		this.isTransactionTimeoutSet = true;
		this.transactionTimeout = (System.currentTimeMillis() / 1000L + seconds);
	}

	public void unsetTransactionTimeout() {
		this.isTransactionTimeoutSet = false;
	}

	protected PreparedStatement getStatement() {
		return this.batchUpdate;
	}

	public CallableStatement prepareCallableStatement(String sql) throws SQLException, HibernateException {
		executeBatch();
		logOpenPreparedStatement();
		return getCallableStatement(this.connectionManager.getConnection(), sql, false);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException, HibernateException {
		return prepareStatement(sql, false);
	}

	public PreparedStatement prepareStatement(String sql, boolean getGeneratedKeys)
			throws SQLException, HibernateException {
		executeBatch();
		logOpenPreparedStatement();
		return getPreparedStatement(this.connectionManager.getConnection(), sql, false, getGeneratedKeys, null, null,
				false);
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException, HibernateException {
		executeBatch();
		logOpenPreparedStatement();
		return getPreparedStatement(this.connectionManager.getConnection(), sql, false, false, columnNames, null,
				false);
	}

	public PreparedStatement prepareSelectStatement(String sql) throws SQLException, HibernateException {
		logOpenPreparedStatement();
		return getPreparedStatement(this.connectionManager.getConnection(), sql, false, false, null, null, false);
	}

	public PreparedStatement prepareQueryStatement(String sql, boolean scrollable, ScrollMode scrollMode)
			throws SQLException, HibernateException {
		logOpenPreparedStatement();
		PreparedStatement ps = getPreparedStatement(this.connectionManager.getConnection(), sql, scrollable,
				scrollMode);

		setStatementFetchSize(ps);
		this.statementsToClose.add(ps);
		this.lastQuery = ps;
		return ps;
	}

	public CallableStatement prepareCallableQueryStatement(String sql, boolean scrollable, ScrollMode scrollMode)
			throws SQLException, HibernateException {
		logOpenPreparedStatement();
		CallableStatement ps = (CallableStatement) getPreparedStatement(this.connectionManager.getConnection(), sql,
				scrollable, false, null, scrollMode, true);

		setStatementFetchSize(ps);
		this.statementsToClose.add(ps);
		this.lastQuery = ps;
		return ps;
	}

	public void abortBatch(SQLException sqle) {
		try {
			if (this.batchUpdate != null) {
				closeStatement(this.batchUpdate);
			}
		} catch (SQLException e) {
			JDBCExceptionReporter.logExceptions(e);
		} finally {
			this.batchUpdate = null;
			this.batchUpdateSQL = null;
		}
	}

	public ResultSet getResultSet(PreparedStatement ps) throws SQLException {
		ResultSet rs = ps.executeQuery();
		this.resultSetsToClose.add(rs);
		logOpenResults();
		return rs;
	}

	public ResultSet getResultSet(CallableStatement ps, Dialect dialect) throws SQLException {
		ResultSet rs = dialect.getResultSet(ps);
		this.resultSetsToClose.add(rs);
		logOpenResults();
		return rs;
	}

	public void closeQueryStatement(PreparedStatement ps, ResultSet rs) throws SQLException {
		boolean psStillThere = this.statementsToClose.remove(ps);
		try {
			if ((rs != null) && (this.resultSetsToClose.remove(rs))) {
				logCloseResults();
				rs.close();
			}
		} finally {
			if (psStillThere) {
				closeQueryStatement(ps);
			}
		}
	}

	public PreparedStatement prepareBatchStatement(String sql) throws SQLException, HibernateException {
		sql = getSQL(sql);
		if (!sql.equals(this.batchUpdateSQL)) {
			this.batchUpdate = prepareStatement(sql);
			this.batchUpdateSQL = sql;
		} else {
			log.debug("reusing prepared statement");
			log(sql);
		}
		return this.batchUpdate;
	}

	public CallableStatement prepareBatchCallableStatement(String sql) throws SQLException, HibernateException {
		if (!sql.equals(this.batchUpdateSQL)) {
			this.batchUpdate = prepareCallableStatement(sql);
			this.batchUpdateSQL = sql;
		}
		return (CallableStatement) this.batchUpdate;
	}

	public void executeBatch() throws HibernateException {
		if (this.batchUpdate != null) {
			System.out.println("*** batchUpdateSQL: " + batchUpdateSQL);
			try {
				try {
					doExecuteBatch(this.batchUpdate);
				} finally {
					closeStatement(this.batchUpdate);
				}
			} catch (SQLException sqle) {
				sqle.printStackTrace();
				throw JDBCExceptionHelper.convert(this.factory.getSQLExceptionConverter(), sqle,
						"Could not execute JDBC batch update", this.batchUpdateSQL);
			} finally {
				this.batchUpdate = null;
				this.batchUpdateSQL = null;
			}
		}
	}

	public void closeStatement(PreparedStatement ps) throws SQLException {
		logClosePreparedStatement();
		closePreparedStatement(ps);
	}

	private void closeQueryStatement(PreparedStatement ps) throws SQLException {
		try {
			if (ps.getMaxRows() != 0) {
				ps.setMaxRows(0);
			}
			if (ps.getQueryTimeout() != 0) {
				ps.setQueryTimeout(0);
			}
		} catch (Exception e) {
			log.warn("exception clearing maxRows/queryTimeout", e);
			return;
		} finally {
			closeStatement(ps);
		}
		if (this.lastQuery == ps) {
			this.lastQuery = null;
		}
	}

	public void closeStatements() {
		try {
			this.releasing = true;
			try {
				if (this.batchUpdate != null) {
					this.batchUpdate.close();
				}
			} catch (SQLException sqle) {
				log.warn("Could not close a JDBC prepared statement", sqle);
			}
			this.batchUpdate = null;
			this.batchUpdateSQL = null;

			Iterator iter = this.resultSetsToClose.iterator();
			while (iter.hasNext()) {
				try {
					logCloseResults();
					((ResultSet) iter.next()).close();
				} catch (SQLException e) {
					log.warn("Could not close a JDBC result set", e);
				} catch (ConcurrentModificationException e) {
					log.info(
							"encountered CME attempting to release batcher; assuming cause is tx-timeout scenario and ignoring");
					break;
				} catch (Throwable e) {
					log.warn("Could not close a JDBC result set", e);
				}
			}
			this.resultSetsToClose.clear();

			iter = this.statementsToClose.iterator();
			while (iter.hasNext()) {
				try {
					closeQueryStatement((PreparedStatement) iter.next());
				} catch (ConcurrentModificationException e) {
					log.info(
							"encountered CME attempting to release batcher; assuming cause is tx-timeout scenario and ignoring");
					break;
				} catch (SQLException e) {
					log.warn("Could not close a JDBC statement", e);
				}
			}
			this.statementsToClose.clear();
		} finally {
			this.releasing = false;
		}
	}

	protected abstract void doExecuteBatch(PreparedStatement paramPreparedStatement)
			throws SQLException, HibernateException;

	private String preparedStatementCountsToString() {
		return " (open PreparedStatements: " + this.openPreparedStatementCount + ", globally: "
				+ globalOpenPreparedStatementCount + ")";
	}

	private String resultSetCountsToString() {
		return " (open ResultSets: " + this.openResultSetCount + ", globally: " + globalOpenResultSetCount + ")";
	}

	private void logOpenPreparedStatement() {
		if (log.isDebugEnabled()) {
			log.debug("about to open PreparedStatement" + preparedStatementCountsToString());
			this.openPreparedStatementCount += 1;
			globalOpenPreparedStatementCount += 1;
		}
	}

	private void logClosePreparedStatement() {
		if (log.isDebugEnabled()) {
			log.debug("about to close PreparedStatement" + preparedStatementCountsToString());
			this.openPreparedStatementCount -= 1;
			globalOpenPreparedStatementCount -= 1;
		}
	}

	private void logOpenResults() {
		if (log.isDebugEnabled()) {
			log.debug("about to open ResultSet" + resultSetCountsToString());
			this.openResultSetCount += 1;
			globalOpenResultSetCount += 1;
		}
	}

	private void logCloseResults() {
		if (log.isDebugEnabled()) {
			log.debug("about to close ResultSet" + resultSetCountsToString());
			this.openResultSetCount -= 1;
			globalOpenResultSetCount -= 1;
		}
	}

	protected SessionFactoryImplementor getFactory() {
		return this.factory;
	}

	private void log(String sql) {
		this.factory.getSettings().getSqlStatementLogger().logStatement(sql, FormatStyle.BASIC);
	}

	private PreparedStatement getPreparedStatement(Connection conn, String sql, boolean scrollable,
			ScrollMode scrollMode) throws SQLException {
		return getPreparedStatement(conn, sql, scrollable, false, null, scrollMode, false);
	}

	private CallableStatement getCallableStatement(Connection conn, String sql, boolean scrollable)
			throws SQLException {
		if ((scrollable) && (!this.factory.getSettings().isScrollableResultSetsEnabled())) {
			throw new AssertionFailure("scrollable result sets are not enabled");
		}
		sql = getSQL(sql);
		log(sql);

		log.trace("preparing callable statement");
		if (scrollable) {
			return conn.prepareCall(sql, 1004, 1007);
		}
		return conn.prepareCall(sql);
	}

	private String getSQL(String sql) {
		sql = this.interceptor.onPrepareStatement(sql);
		if ((sql == null) || (sql.length() == 0)) {
			throw new AssertionFailure("Interceptor.onPrepareStatement() returned null or empty string.");
		}
		return sql;
	}

	private PreparedStatement getPreparedStatement(Connection conn, String sql, boolean scrollable,
			boolean useGetGeneratedKeys, String[] namedGeneratedKeys, ScrollMode scrollMode, boolean callable)
			throws SQLException {
		if ((scrollable) && (!this.factory.getSettings().isScrollableResultSetsEnabled())) {
			throw new AssertionFailure("scrollable result sets are not enabled");
		}
		if ((useGetGeneratedKeys) && (!this.factory.getSettings().isGetGeneratedKeysEnabled())) {
			throw new AssertionFailure("getGeneratedKeys() support is not enabled");
		}
		sql = getSQL(sql);
		log(sql);

		log.trace("preparing statement");
		PreparedStatement result;
		if (scrollable) {
			if (callable) {
				result = conn.prepareCall(sql, scrollMode.toResultSetType(), 1007);
			} else {
				result = conn.prepareStatement(sql, scrollMode.toResultSetType(), 1007);
			}
		} else {
			if (useGetGeneratedKeys) {
				result = conn.prepareStatement(sql, 1);
			} else {
				if (namedGeneratedKeys != null) {
					result = conn.prepareStatement(sql, namedGeneratedKeys);
				} else {
					if (callable) {
						result = conn.prepareCall(sql);
					} else {
						result = conn.prepareStatement(sql);
					}
				}
			}
		}
		setTimeout(result);
		if (this.factory.getStatistics().isStatisticsEnabled()) {
			this.factory.getStatisticsImplementor().prepareStatement();
		}
		return result;
	}

	private void setTimeout(PreparedStatement result) throws SQLException {
		if (this.isTransactionTimeoutSet) {
			int timeout = (int) (this.transactionTimeout - System.currentTimeMillis() / 1000L);
			if (timeout <= 0) {
				throw new TransactionException("transaction timeout expired");
			}
			result.setQueryTimeout(timeout);
		}
	}

	private void closePreparedStatement(PreparedStatement ps) throws SQLException {
		try {
			log.trace("closing statement");
			ps.close();
			if (this.factory.getStatistics().isStatisticsEnabled()) {
				this.factory.getStatisticsImplementor().closeStatement();
			}
		} finally {
			if (!this.releasing) {
				this.connectionManager.afterStatement();
			}
		}
	}

	private void setStatementFetchSize(PreparedStatement statement) throws SQLException {
		Integer statementFetchSize = this.factory.getSettings().getJdbcFetchSize();
		if (statementFetchSize != null) {
			statement.setFetchSize(statementFetchSize.intValue());
		}
	}

	public Connection openConnection() throws HibernateException {
		log.debug("opening JDBC connection");
		try {
			return this.factory.getConnectionProvider().getConnection();
		} catch (SQLException sqle) {
			throw JDBCExceptionHelper.convert(this.factory.getSQLExceptionConverter(), sqle, "Cannot open connection");
		}
	}

	public void closeConnection(Connection conn) throws HibernateException {
		if (conn == null) {
			log.debug("found null connection on AbstractBatcher#closeConnection");

			return;
		}
		if (log.isDebugEnabled()) {
			log.debug("closing JDBC connection" + preparedStatementCountsToString() + resultSetCountsToString());
		}
		try {
			if (!conn.isClosed()) {
				JDBCExceptionReporter.logAndClearWarnings(conn);
			}
			this.factory.getConnectionProvider().closeConnection(conn);
		} catch (SQLException sqle) {
			throw JDBCExceptionHelper.convert(this.factory.getSQLExceptionConverter(), sqle, "Cannot close connection");
		}
	}

	public void cancelLastQuery() throws HibernateException {
		try {
			if (this.lastQuery != null) {
				this.lastQuery.cancel();
			}
		} catch (SQLException sqle) {
			throw JDBCExceptionHelper.convert(this.factory.getSQLExceptionConverter(), sqle, "Cannot cancel query");
		}
	}

	public boolean hasOpenResources() {
		return (this.resultSetsToClose.size() > 0) || (this.statementsToClose.size() > 0);
	}

	public String openResourceStatsAsString() {
		return preparedStatementCountsToString() + resultSetCountsToString();
	}
}
