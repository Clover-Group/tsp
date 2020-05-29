package ru.itclover.tsp;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.io.jdbc.split.ParameterValuesProvider;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

public class JDBCInputFormatProps extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

	private static final long serialVersionUID = 1L;
	private static final String USER_PROP = "user";
	private static final String PASSWORD_PROP = "password";
	private static final Logger LOG = LoggerFactory.getLogger(JDBCInputFormatProps.class);

	private String drivername;
	private String dbURL;
	private final Properties props = new Properties();
	private String queryTemplate;
	private int resultSetType;
	private int resultSetConcurrency;
	private RowTypeInfo rowTypeInfo;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient ResultSet resultSet;
	private int fetchSize;

	private boolean hasNext;
	private Object[][] parameterValues;

	public JDBCInputFormatProps() {
	}

	@Override
	public RowTypeInfo getProducedType() {
		return rowTypeInfo;
	}

	@Override
	public void configure(Configuration parameters) {
		//do nothing here
	}

	@Override
	public void openInputFormat() {
		//called once per inputFormat (on open)
		try {
			Class.forName(drivername);
			dbConn = DriverManager.getConnection(dbURL, props);
			statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
			if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
				statement.setFetchSize(fetchSize);
			}
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
		}
	}

	@Override
	public void closeInputFormat() {
		//called once per inputFormat (on close)
		try {
			if (statement != null) {
				statement.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat Statement couldn't be closed - " + se.getMessage());
		} finally {
			statement = null;
		}

		try {
			if (dbConn != null) {
				dbConn.close();
			}
		} catch (SQLException se) {
			LOG.info("Inputformat couldn't be closed - " + se.getMessage());
		} finally {
			dbConn = null;
		}

		parameterValues = null;
	}

	/**
	 * Connects to the source database and executes the query in a <b>parallel
	 * fashion</b> if
	 * this {@link InputFormat} is built using a parameterized query (i.e. using
	 * a {@link PreparedStatement})
	 * and a proper {@link ParameterValuesProvider}, in a <b>non-parallel
	 * fashion</b> otherwise.
	 *
	 * @param inputSplit which is ignored if this InputFormat is executed as a
	 *        non-parallel source,
	 *        a "hook" to the query parameters otherwise (using its
	 *        <i>splitNumber</i>)
	 * @throws IOException if there's an error during the execution of the query
	 */
	@Override
	public void open(InputSplit inputSplit) throws IOException {
		try {
			if (inputSplit != null && parameterValues != null) {
				for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
					Object param = parameterValues[inputSplit.getSplitNumber()][i];
					if (param instanceof String) {
						statement.setString(i + 1, (String) param);
					} else if (param instanceof Long) {
						statement.setLong(i + 1, (Long) param);
					} else if (param instanceof Integer) {
						statement.setInt(i + 1, (Integer) param);
					} else if (param instanceof Double) {
						statement.setDouble(i + 1, (Double) param);
					} else if (param instanceof Boolean) {
						statement.setBoolean(i + 1, (Boolean) param);
					} else if (param instanceof Float) {
						statement.setFloat(i + 1, (Float) param);
					} else if (param instanceof BigDecimal) {
						statement.setBigDecimal(i + 1, (BigDecimal) param);
					} else if (param instanceof Byte) {
						statement.setByte(i + 1, (Byte) param);
					} else if (param instanceof Short) {
						statement.setShort(i + 1, (Short) param);
					} else if (param instanceof Date) {
						statement.setDate(i + 1, (Date) param);
					} else if (param instanceof Time) {
						statement.setTime(i + 1, (Time) param);
					} else if (param instanceof Timestamp) {
						statement.setTimestamp(i + 1, (Timestamp) param);
					} else if (param instanceof Array) {
						statement.setArray(i + 1, (Array) param);
					} else {
						//extends with other types if needed
						throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
					}
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
				}
			}
			resultSet = statement.executeQuery();
			hasNext = resultSet.next();
		} catch (SQLException se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	/**
	 * Closes all resources used.
	 *
	 * @throws IOException Indicates that a resource could not be closed.
	 */
	@Override
	public void close() throws IOException {
		if (resultSet == null) {
			return;
		}
		try {
			resultSet.close();
		} catch (SQLException se) {
			LOG.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
		}
	}

	/**
	 * Checks whether all data has been read.
	 *
	 * @return boolean value indication whether all data has been read.
	 * @throws IOException
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return !hasNext;
	}

	/**
	 * Stores the next resultSet row in a tuple.
	 *
	 * @param row row to be reused.
	 * @return row containing next {@link Row}
	 * @throws java.io.IOException
	 */
	@Override
	public Row nextRecord(Row row) throws IOException {
		try {
			if (!hasNext) {
				return null;
			}
			for (int pos = 0; pos < row.getArity(); pos++) {
				row.setField(pos, resultSet.getObject(pos + 1));
			}
			//update hasNext after we've read the record
			hasNext = resultSet.next();
			return row;
		} catch (SQLException se) {
			throw new IOException("Couldn't read data - " + se.getMessage(), se);
		} catch (NullPointerException npe) {
			throw new IOException("Couldn't access resultSet", npe);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (parameterValues == null) {
			return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
		}
		GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = new GenericInputSplit(i, ret.length);
		}
		return ret;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@VisibleForTesting
	PreparedStatement getStatement() {
		return statement;
	}

	/**
	 * A builder used to set parameters to the output format's configuration in a fluent way.
	 * @return builder
	 */
	public static JDBCInputFormatPropsBuilder buildJDBCInputFormat() {
		return new JDBCInputFormatPropsBuilder();
	}

	/**
	 * Builder for a {@link JDBCInputFormatProps}.
	 */
	public static class JDBCInputFormatPropsBuilder {
		private final JDBCInputFormatProps format;

		public JDBCInputFormatPropsBuilder() {
			this.format = new JDBCInputFormatProps();
			//using TYPE_FORWARD_ONLY for high performance reads
			this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
			this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
		}
		
		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder addProperties(Properties props) {
			Enumeration<?> names = props.propertyNames();
			while (names.hasMoreElements()) {
				String name = names.nextElement().toString();
				format.props.setProperty(name, props.getProperty(name));
			}
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setUsername(String username) {
			format.props.setProperty(USER_PROP, username);
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setPassword(String password) {
			format.props.setProperty(PASSWORD_PROP, password);
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setDrivername(String drivername) {
			format.drivername = drivername;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setDBUrl(String dbURL) {
			format.dbURL = dbURL;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setQuery(String query) {
			format.queryTemplate = query;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setResultSetType(int resultSetType) {
			format.resultSetType = resultSetType;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setResultSetConcurrency(int resultSetConcurrency) {
			format.resultSetConcurrency = resultSetConcurrency;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setParametersProvider(ParameterValuesProvider parameterValuesProvider) {
			format.parameterValues = parameterValuesProvider.getParameterValues();
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
			format.rowTypeInfo = rowTypeInfo;
			return this;
		}

		public JDBCInputFormatProps.JDBCInputFormatPropsBuilder setFetchSize(int fetchSize) {
			Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
				"Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
			format.fetchSize = fetchSize;
			return this;
		}

		public JDBCInputFormatProps finish() {
			if (format.props.getProperty(USER_PROP) == null) {
				LOG.info("Username was not supplied separately.");
			}
			if (format.props.getProperty(PASSWORD_PROP) == null) {
				LOG.info("Password was not supplied separately.");
			}
			if (format.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied");
			}
			if (format.queryTemplate == null) {
				throw new IllegalArgumentException("No query supplied");
			}
			if (format.drivername == null) {
				throw new IllegalArgumentException("No driver supplied");
			}
			if (format.rowTypeInfo == null) {
				throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
			}
			if (format.parameterValues == null) {
				LOG.debug("No input splitting configured (data will be read with parallelism 1).");
			}
			return format;
		}

	}

}