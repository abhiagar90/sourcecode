package simpledb.index.inverted;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import simpledb.index.btree.BTreeIndex;
import simpledb.parse.BadSyntaxException;
import simpledb.query.Constant;
import simpledb.query.StringConstant;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class InvertedIndex extends BTreeIndex {

	public InvertedIndex(String idxname, Schema leafsch, Transaction tx) {
		super(idxname+"_invvni", leafsch, tx); //_invvni is added by us for uniqueness!!
		// TODO Auto-generated constructor stub
	}

	public void insert(Constant dataval, RID datarid) {
	
		StringConstant fullstr = (StringConstant) dataval;
		String value = fullstr.toString();
		String[] splited = value.split("\\s+");
		Set<String> mySet = new HashSet<String>(Arrays.asList(splited)); //removing duplicates
		for (String each : mySet)
		{
			System.out.println("going to insert index for"+each.toLowerCase() +" for datarid "+datarid);
			super.insert(new StringConstant(each.toLowerCase()), datarid); //TODO:converting each to lowercase
			//super.close();
		}
	}
	
	public void delete(Constant dataval, RID datarid) {
		StringConstant fullstr = (StringConstant) dataval;
		String value = fullstr.toString();
		String[] splited = value.split("\\s+");
		for (String each : splited)
		{
			System.out.println("going to delete index for "+each.toLowerCase() +" for datarid "+datarid);
			super.delete(new StringConstant(each.toLowerCase()), datarid); //TODO: tolowercase while deleting even
		}
	   }	

	//TODO: searchkey is one word for now
	public void beforeFirst(Constant searchkey) {
	     if (searchkey instanceof StringConstant)
	     {
	    	StringConstant fullstr = (StringConstant) searchkey;
	 		String value = fullstr.toString();
	 		String[] splited = value.split("\\s+");
	 		if(splited.length != 1)
	 		{
	 			System.out.println("The search should be on one word for now");
	 			throw new BadSyntaxException();
	 		}
	 		else
	 		{
	 			super.beforeFirst(searchkey);
	 		}
	     }
	     else
	    	 throw new BadSyntaxException();
	    	 
	   }

}
