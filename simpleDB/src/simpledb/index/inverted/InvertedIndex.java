package simpledb.index.inverted;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.xml.bind.helpers.ParseConversionEventImpl;

import simpledb.index.btree.BTreeIndex;
import simpledb.parse.BadSyntaxException;
import simpledb.parse.Parser;
import simpledb.query.Constant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class InvertedIndex extends BTreeIndex {

	private String newIndexName;

	public InvertedIndex(String idxname, Schema leafsch, Transaction tx) {

		super(idxname + "_invvni", leafsch, tx); // _invvni is added by us for
													// uniqueness!!
		this.newIndexName = idxname + "_invvni";
		// TODO: change name unqiueness too long
		// TODO Auto-generated constructor stub
	}

	public void insert(Constant dataval, RID datarid) {

		StringConstant fullstr = (StringConstant) dataval;
		String value = fullstr.toString();
		String filteredValue = removePuntucationMarks(value);
		String[] splited = filteredValue.split("\\s+");
		Set<String> mySet = new HashSet<String>(Arrays.asList(splited)); // removing
																			// duplicates
		for (String each : mySet) {
			System.out.println("going to insert index for" + each.toLowerCase()
					+ " for datarid " + datarid);
			super.insert(new StringConstant(each.toLowerCase()), datarid); // TODO:converting
																			// each
																			// to
																			// lowercase
			// super.close();
		}
		System.out.println("HHHHHHHH now the height of the btree is: "
				+ super.getHeight());
	}

	public void delete(Constant dataval, RID datarid) {
		StringConstant fullstr = (StringConstant) dataval;
		String value = fullstr.toString();
		String filteredValue = removePuntucationMarks(value);
		String[] splited = filteredValue.split("\\s+");
		for (String each : splited) {
			System.out.println("going to delete index for "
					+ each.toLowerCase() + " for datarid " + datarid);
			super.delete(new StringConstant(each.toLowerCase()), datarid); // TODO:
																			// tolowercase
																			// while
																			// deleting
																			// even
		}
	}

	// TODO: searchkey is one word for now
	public void beforeFirst(Constant searchkey) {
		if (searchkey instanceof StringConstant) {
			StringConstant fullstr = (StringConstant) searchkey;
			String value = fullstr.toString();
			// TODO: also a check for punctuations
			// TODO: change this based on their reply
			String[] splited = value.split("\\s+");
			if (splited.length != 1) {
				System.out.println("The search should be on one word for now");
				throw new BadSyntaxException();
			} else {
				super.beforeFirst(new StringConstant(splited[0].toLowerCase()));
			}
		} else
			throw new BadSyntaxException();

	}

	public String removePuntucationMarks(String str) {

		str = str.replaceAll("[-+^?.\'\"!,:;]*", "");
		return str;
	}

}
