package de.brohlsoft.jdbcextract;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.commons.dbutils.DbUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.Property;

public class Extract {

	public static void main(final String[] args) throws SQLException {
		@SuppressWarnings("resource")
		final Scanner sc = new Scanner(System.in);
		final String line = sc.nextLine();
		final JSONObject config = new JSONObject(line);
		final JSONObject dst = config.getJSONObject("dst");
		final JSONObject src = config.getJSONObject("src");
		final String dstUrl = dst.getString("url");
		final List<String> dstPrepareSql = arrayToList(dst.getJSONArray("prepare_sql"));
		final String dstInsertSql = dst.getString("insert_sql");
		final List<String> driverClassNames = arrayToList(config.getJSONArray("driver_class_names"));
		final String srcUrl = src.getString("url");
		final Properties srcProperties = Property.toProperties(src.getJSONObject("properties"));
		final String srcSelectSql = src.getString("select_sql");
		final int batchSize = config.optInt("batch_size", 1000);

		init(driverClassNames);
		extract(dstUrl, dstPrepareSql, dstInsertSql, srcUrl, srcProperties, srcSelectSql, batchSize);

	}

	private static List<String> arrayToList(final JSONArray array) {
		final int size = array.length();
		final List<String> out = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			out.add(array.getString(i));
		}
		return out;
	}

	private static void extract(final String dstUrl, final List<String> dstPrepareSql, final String dstInsertSql,
			final String srcUrl, final Properties srcProperties, final String srcSelectSql, final int batchSize)
			throws SQLException {
		try (Connection dst = DriverManager.getConnection(dstUrl)) {
			dst.setAutoCommit(false);
			executeMany(dstPrepareSql, dst);
			final PreparedStatement insert = dst.prepareStatement(dstInsertSql);
			try (Connection src = DriverManager.getConnection(srcUrl, srcProperties)) {
				final Statement select = src.createStatement();
				final ResultSet rs = select.executeQuery(srcSelectSql);
				final int columns = rs.getMetaData().getColumnCount();
				int rowsLeft = batchSize;
				while (rs.next()) {
					for (int i = 1; i <= columns; i++) {
						final Object obj = rs.getObject(i);
						insert.setObject(i, obj);
					}
					insert.addBatch();
					if (rowsLeft <= 0) {
						insert.executeBatch();
						rowsLeft = batchSize;
					} else {
						rowsLeft--;
					}
				}
			}
			dst.commit();
		}
	}

	private static void executeMany(final List<String> sqlStatements, final Connection connection) throws SQLException {
		final Statement st = connection.createStatement();

		for (final String sql : sqlStatements) {
			st.executeUpdate(sql);
		}
	}

	private static boolean init(final List<String> driverClassNames) {
		boolean ok = true;
		for (final String name : driverClassNames) {
			ok &= DbUtils.loadDriver(name);
		}
		return ok;
	}

}
