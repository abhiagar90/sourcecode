package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.Schema;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
	private Lexer lex;

	public Parser(String s) {
		lex = new Lexer(s);
	}

	// Methods for parsing predicates, terms, expressions, constants, and fields

	public String field() {
		return lex.eatId();
	}

	public Constant constant() {
		if (lex.matchStringConstant())
			return new StringConstant(lex.eatStringConstant());
		else
			return new IntConstant(lex.eatIntConstant());
	}

	public Expression expression() {
		if (lex.matchId())
			return new FieldNameExpression(field());
		else
			return new ConstantExpression(constant());
	}

	//TODO: remove or morph
	public Term oldterm(Collection<String> tables) {
		Expression lhs = expression();
		if (lex.matchKeyword("contains")) {
			ArrayList<String> operands = new ArrayList<String>();
			ArrayList<String> operators = new ArrayList<String>();
			lex.eatKeyword("contains");
			lex.eatDelim('(');

			operands.add(lex.eatStringConstant());
			do {
				if (lex.matchKeyword("and")) {
					lex.eatKeyword("and");
					operators.add("and");
				} else if (lex.matchKeyword("or")) {
					lex.eatKeyword("or");
					operators.add("or");
				} else if (lex.matchDelim(')')) {
					lex.eatDelim(')');
					break;
				} else {
					throw new BadSyntaxException();
				}
				operands.add(lex.eatStringConstant());

			} while (true);

			return new Term(lhs, operands, operators, tables);

		} else {
			lex.eatDelim('=');
			Expression rhs = expression();
			return new Term(lhs, rhs);
		}
	}

	//TODO: morph now!!
	public Term term(Collection<String> tables) {
		Expression lhs = expression();
		if (lex.matchKeyword("contains")) {
			ArrayList<String> operands = new ArrayList<String>();
			ArrayList<String> operators = new ArrayList<String>();
			lex.eatKeyword("contains");

			do {
				lex.eatDelim('(');
				operands.add(lex.eatStringConstant());
				lex.eatDelim(')');

				if (lex.matchKeyword("and")) {
					lex.eatKeyword("and");
					operators.add("and");
				} else if (lex.matchKeyword("or")) {
					lex.eatKeyword("or");
					operators.add("or");
				} else if (lex.matchDelim(';')) { 
					break; //TODO: if contains works with = type expressions also, then change
					//TODO: precedence of or and and
				}
			} while (true);

			return new Term(lhs, operands, operators, tables);

		} else {
			lex.eatDelim('=');
			Expression rhs = expression();
			return new Term(lhs, rhs);
		}
	}

	public Predicate predicate(Collection<String> tables) {
		Predicate pred = new Predicate(term(tables));
		if (lex.matchKeyword("and")) {
			lex.eatKeyword("and");
			pred.conjoinWith(predicate(tables));
		}
		return pred;
	}

	// Methods for parsing queries

	// Contains can be here
	public QueryData query() {
		lex.eatKeyword("select");
		Collection<String> fields = selectList();
		lex.eatKeyword("from");
		Collection<String> tables = tableList();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate(tables);
		}
		return new QueryData(fields, tables, pred);
	}

	private Collection<String> selectList() {
		Collection<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(selectList());
		}
		return L;
	}

	private Collection<String> tableList() {
		Collection<String> L = new ArrayList<String>();
		L.add(lex.eatId());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(tableList());
		}
		return L;
	}

	// Methods for parsing the various update commands

	public Object updateCmd() {
		if (lex.matchKeyword("insert"))
			return insert();
		else if (lex.matchKeyword("delete"))
			return delete();
		else if (lex.matchKeyword("update"))
			return modify();
		else
			return create();
	}

	private Object create() {
		lex.eatKeyword("create");
		if (lex.matchKeyword("table"))
			return createTable();
		else if (lex.matchKeyword("view"))
			return createView();
		else
			return createIndex();
	}

	// Method for parsing delete commands

	public DeleteData delete() {
		lex.eatKeyword("delete");
		lex.eatKeyword("from");
		String tblname = lex.eatId();
		// TODO: do check this
		Collection<String> tables = Arrays.asList(tblname);
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate(tables);
		}
		return new DeleteData(tblname, pred);
	}

	// Methods for parsing insert commands

	public InsertData insert() {
		lex.eatKeyword("insert");
		lex.eatKeyword("into");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		List<String> flds = fieldList();
		lex.eatDelim(')');
		lex.eatKeyword("values");
		lex.eatDelim('(');
		List<Constant> vals = constList();
		lex.eatDelim(')');
		return new InsertData(tblname, flds, vals);
	}

	private List<String> fieldList() {
		List<String> L = new ArrayList<String>();
		L.add(field());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(fieldList());
		}
		return L;
	}

	private List<Constant> constList() {
		List<Constant> L = new ArrayList<Constant>();
		L.add(constant());
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			L.addAll(constList());
		}
		return L;
	}

	// Method for parsing modify commands

	public ModifyData modify() {
		lex.eatKeyword("update");
		String tblname = lex.eatId();
		// TODO: check this
		Collection<String> tables = Arrays.asList(tblname);
		lex.eatKeyword("set");
		String fldname = field();
		lex.eatDelim('=');
		Expression newval = expression();
		Predicate pred = new Predicate();
		if (lex.matchKeyword("where")) {
			lex.eatKeyword("where");
			pred = predicate(tables);
		}
		return new ModifyData(tblname, fldname, newval, pred);
	}

	// Method for parsing create table commands

	public CreateTableData createTable() {
		lex.eatKeyword("table");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		Schema sch = fieldDefs();
		lex.eatDelim(')');
		return new CreateTableData(tblname, sch);
	}

	private Schema fieldDefs() {
		Schema schema = fieldDef();
		if (lex.matchDelim(',')) {
			lex.eatDelim(',');
			Schema schema2 = fieldDefs();
			schema.addAll(schema2);
		}
		return schema;
	}

	private Schema fieldDef() {
		String fldname = field();
		return fieldType(fldname);
	}

	private Schema fieldType(String fldname) {
		Schema schema = new Schema();
		if (lex.matchKeyword("int")) {
			lex.eatKeyword("int");
			schema.addIntField(fldname);
		} else {
			lex.eatKeyword("varchar");
			lex.eatDelim('(');
			int strLen = lex.eatIntConstant();
			lex.eatDelim(')');
			schema.addStringField(fldname, strLen);
		}
		return schema;
	}

	// Method for parsing create view commands

	public CreateViewData createView() {
		lex.eatKeyword("view");
		String viewname = lex.eatId();
		lex.eatKeyword("as");
		QueryData qd = query();
		return new CreateViewData(viewname, qd);
	}

	// Method for parsing create index commands

	public CreateIndexData createIndex() {
		lex.eatKeyword("index");
		String idxname = lex.eatId();
		String idxtype = lex.eatIndexType();
		lex.eatKeyword("on");
		String tblname = lex.eatId();
		lex.eatDelim('(');
		String fldname = field();
		lex.eatDelim(')');
		return new CreateIndexData(idxname, tblname, fldname, idxtype);
	}
	
	
}
