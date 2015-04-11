package simpledb.query;

import java.util.ArrayList;
import java.util.Collection;

import simpledb.index.Index;
import simpledb.metadata.IndexInfo;
import simpledb.parse.BadSyntaxException;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

/**
 * A term is a comparison between two expressions.
 * 
 * @author Edward Sciore
 * 
 */
public class Term {
	private Expression lhs, rhs;

	/**
	 * For B Tree inverted index adding instance variables TODO: change names,
	 * remove comments
	 */
	private boolean isContains = false;
	private ArrayList<String> operators = null, operands = null;
	private ArrayList<RID> ridsSatisfyingTerm = null; // list of RIDs satisfying
														// this term

	private boolean isEvaluated = false;
	private Collection<String> tables = null;
	private String tableName = null; // table to which lhs of term belongs

	/**
	 * Creates a new term that compares two expressions for equality.
	 * 
	 * @param lhs
	 *            the LHS expression
	 * @param rhs
	 *            the RHS expression
	 */
	public Term(Expression lhs, Expression rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}

	public Term(Expression lhs, ArrayList<String> operands,
			ArrayList<String> operators, Collection<String> tables) {
		this.lhs = lhs;
		this.operators = operators;
		this.operands = operands;
		this.isContains = true;
		this.tables = tables; // this is needed to fetch the index information
								// when the term is evaluated for "contains"
	}

	/**
	 * Calculates the extent to which selecting on the term reduces the number
	 * of records output by a query. For example if the reduction factor is 2,
	 * then the term cuts the size of the output in half.
	 * 
	 * @param p
	 *            the query's plan
	 * @return the integer reduction factor.
	 */
	public int reductionFactor(Plan p) {
		String lhsName, rhsName;
		if (lhs.isFieldName() && rhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			rhsName = rhs.asFieldName();
			return Math.max(p.distinctValues(lhsName),
					p.distinctValues(rhsName));
		}
		if (lhs.isFieldName()) {
			lhsName = lhs.asFieldName();
			return p.distinctValues(lhsName);
		}
		if (rhs.isFieldName()) {
			rhsName = rhs.asFieldName();
			return p.distinctValues(rhsName);
		}
		// otherwise, the term equates constants
		if (lhs.asConstant().equals(rhs.asConstant()))
			return 1;
		else
			return Integer.MAX_VALUE;
	}

	/**
	 * Determines if this term is of the form "F=c" where F is the specified
	 * field and c is some constant. If so, the method returns that constant. If
	 * not, the method returns null.
	 * 
	 * @param fldname
	 *            the name of the field
	 * @return either the constant or null
	 */
	public Constant equatesWithConstant(String fldname) {
		if (lhs.isFieldName() && lhs.asFieldName().equals(fldname)
				&& rhs.isConstant())
			return rhs.asConstant();
		else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname)
				&& lhs.isConstant())
			return lhs.asConstant();
		else
			return null;
	}

	/**
	 * Determines if this term is of the form "F1=F2" where F1 is the specified
	 * field and F2 is another field. If so, the method returns the name of that
	 * field. If not, the method returns null.
	 * 
	 * @param fldname
	 *            the name of the field
	 * @return either the name of the other field, or null
	 */
	public String equatesWithField(String fldname) {
		if (lhs.isFieldName() && lhs.asFieldName().equals(fldname)
				&& rhs.isFieldName())
			return rhs.asFieldName();
		else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname)
				&& lhs.isFieldName())
			return lhs.asFieldName();
		else
			return null;
	}

	/**
	 * Returns true if both of the term's expressions apply to the specified
	 * schema.
	 * 
	 * @param sch
	 *            the schema
	 * @return true if both expressions apply to the schema
	 */
	public boolean appliesTo(Schema sch) {
		return lhs.appliesTo(sch) && rhs.appliesTo(sch);
	}

	/**
	 * Returns true if both of the term's expressions evaluate to the same
	 * constant, with respect to the specified scan.
	 * 
	 * @param s
	 *            the scan
	 * @return true if both expressions have the same value in the scan 
	 * TODO:
	 *         remove comments , etc. etc. etc.
	 */
	public boolean isSatisfied(Scan s) {

		if (this.isContains) {
			// TODO: shouldn't this be somewhere else!!
			UpdateScan u = (UpdateScan) s;
			Transaction tx = new Transaction();
			// if not evaluated then first evaluate
			if (!isEvaluated) {
				ArrayList<ArrayList<RID>> operandRIDs = new ArrayList<ArrayList<RID>>();
				// 0. find out what table does the lhs attrib belongs to
				// 1. check if inverted index exists on lhs value, if not throw
				// some error
				// 2. Read the index from metadatamanager.
				// 3. Read the RIDs for each term in the rhs of contains
				// 4. perform all "and" operations
				// 5. perform all "or" operations
				// 6. store the resulting RIDs in "ridsSatisfyingTerm" and set
				// evaluated = true
				
				// step 0
				findTable(tx);

				// step 1
				if (!checkIndex(tx)) {
					// give error, index doesn't exist
					System.err.println("Error : No inverted index on field \'"
							+ lhs.toString() + "\' to run contains query ");
					throw new BadSyntaxException();
				}
				// step 2
				IndexInfo ii = SimpleDB.mdMgr()
						.getIndexInfo(this.tableName, tx).get(lhs.toString());
				Index idx = ii.open();
				// step 3
				for (String opr : operands) {
					idx.beforeFirst(new StringConstant(opr.toLowerCase())); 
					//TODO: redoing the lowercase here!! Also done in InvertedIndex
					ArrayList<RID> ridlist = new ArrayList<RID>();
					while (idx.next()) {
						ridlist.add(idx.getDataRid());
					}
					operandRIDs.add(ridlist);
				}
				idx.close(); //TODO: check added this because of their bugs
				// step 4

				while (operators.size() > 0) {
					int oprn_index;
					// get operator preferably & then |
					if (operators.contains("and")) {
						oprn_index = operators.indexOf("and");
					} else {
						oprn_index = operators.indexOf("or");
					}
					ArrayList<RID> op1, op2, res;
					op1 = operandRIDs.get(oprn_index);
					op2 = operandRIDs.get(oprn_index + 1);

					if (operators.get(oprn_index).equals("and")) {
						res = intersect(op1, op2);

					} else {
						res = union(op1, op2);
					}
					operators.remove(oprn_index);
					operandRIDs.remove(oprn_index);
					operandRIDs.remove(oprn_index);
					operandRIDs.add(oprn_index, res);

				}
				// step 6
				ridsSatisfyingTerm = operandRIDs.get(0);
				isEvaluated = true;
			}

			// check if current RID under question satisfies the contains
			// construct
			RID r = u.getRid();
			tx.commit();
			return ridsSatisfyingTerm.contains(r);

		} else {
			Constant lhsval = lhs.evaluate(s);
			Constant rhsval = rhs.evaluate(s);
			return rhsval.equals(lhsval);
		}
	}

	public String toString() {
		return lhs.toString() + "=" + rhs.toString();
	}

	// TODO: change something
	private boolean checkIndex(Transaction tx) {
		IndexInfo ii = SimpleDB.mdMgr().getIndexInfo(this.tableName, tx)
				.get(lhs.toString());
		if (ii == null)
			return false;
		else if (!ii.getIdxType().equals("btreeinv")) {
			System.err.println("Error : Not an inverted index");
			// throw new BadSyntaxException(); //I think it should be this
			// //TODO
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Intersection of the 2 postings list
	 * 
	 * @param list1
	 * @param list2
	 * @return TODO: change something
	 */
	private ArrayList<RID> intersect(ArrayList<RID> list1, ArrayList<RID> list2) {
		ArrayList<RID> result = new ArrayList<RID>();
		for (RID i : list1) {
			if (list2.contains(i))
				result.add(i);
		}
		return result;
	}

	private ArrayList<RID> union(ArrayList<RID> op1, ArrayList<RID> op2) {

		ArrayList<RID> result = new ArrayList<RID>();
		result.addAll(op1);
		for (RID i : op2) {
			if (!result.contains(i))
				result.add(i);
		}
		return result;
	}

	/**
	 * Calling Table Name To which the field belongs
	 * 
	 */
	public String findTable(Transaction tx) {

		for (String table : this.tables) {
			TableInfo ti = SimpleDB.mdMgr().getTableInfo(table, tx);
			System.out.println(this.lhs.toString());
			if (ti.schema().hasField(this.lhs.toString())) {
				this.tableName = table;
				return table;
			}
		}
		// TODO: change println
		System.err.println("Error : Attribute \'" + this.lhs.toString()
				+ "\' doesn't belong to tables mentioned in the from clause.");
		throw new BadSyntaxException();
	}

}
