package simpledb.index;

import static simpledb.metadata.TableMgr.MAX_NAME;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.record.TableInfo;

/* This is a version of the StudentMajor program that
 * accesses the SimpleDB classes directly (instead of
 * connecting to it as a JDBC client).  You can run it
 * without having the server also run.
 * 
 * These kind of programs are useful for debugging
 * your changes to the SimpleDB source code.
 */

public class HeightManageBtree {

	public static String heightTableName = "hhhhh22";

	public void insertNewHeightEntry(int height, String indexname) {
		Transaction tx = new Transaction();
		Schema sch = new Schema();
		sch.addIntField("height");
		sch.addStringField("indexname", MAX_NAME);
		TableInfo ti = new TableInfo(heightTableName, sch);
		TableScan ts = new TableScan(ti, tx);
		ts.insert();
		ts.setInt("height", height);
		ts.setString("indexname", indexname);
		ts.close();
		tx.commit();
	}

	public int getHeight(String indexname) {
		// analogous to the connection
		Transaction tx = new Transaction();
		Schema sch = new Schema();
		sch.addIntField("height");
		sch.addStringField("indexname", MAX_NAME);
		TableInfo ti = new TableInfo(heightTableName, sch);
		TableScan ts = new TableScan(ti, tx);
		int height = 0;
		while (ts.next())
			if (((String) (ts.getVal("indexname").asJavaVal()))
					.equals(indexname)) {
				IntConstant height_inc = (IntConstant) ts.getVal("height");
				height = (Integer) height_inc.asJavaVal();
				break;
			}
		ts.close();
		tx.commit();
		return height;
	}

	public boolean updateHeight(int height, String idxname) {
		if (deleteHeight(idxname)) {
			insertNewHeightEntry(height, idxname);
			return true;
		}
		return false;
	}

	public boolean deleteHeight(String idxname) {
		boolean flag = false;
		Transaction tx = new Transaction();
		Schema sch = new Schema();
		sch.addIntField("height");
		sch.addStringField("indexname", MAX_NAME);
		TableInfo ti = new TableInfo(heightTableName, sch);
		TableScan ts = new TableScan(ti, tx);
		while (ts.next())
			if (((String) (ts.getVal("indexname").asJavaVal())).equals(idxname)) {
				ts.delete();
				flag = true;
				break;
			}
		ts.close();
		tx.commit();
		return flag;
	}

}
