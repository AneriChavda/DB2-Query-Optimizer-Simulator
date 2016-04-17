import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import com.sun.istack.internal.Nullable;

/**
 * CS 267 - Project - Implements create index, drop index, list table, and
 * exploit the index in select statements.
 */
public class DBMS {
	private static final String COMMAND_FILE_LOC = "Commands.txt";
	private static final String OUTPUT_FILE_LOC = "Output.txt";

	private static final String TABLE_FOLDER_NAME = "tables";
	private static final String TABLE_FILE_EXT = ".tab";
	private static final String INDEX_FILE_EXT = ".idx";
	private static final String DESC = "DESC";
	private static final String SEMI_COLON = ";";
	private static final String NULL = "-";
	private static final String FROM = "FROM";
	private static final String WHERE = "WHERE";
	private static final String COMMA = ",";
	private static final String ORDER = "ORDER";
	private static final String AND = "AND";
	private static final String OR = "OR";
	private static final String IN = "IN";
	private static final String OPENING_BRACE = "(";
	private static final String CLOSING_BRACE = ")";
	private static boolean evaluateIndex = false;
	private static boolean checkForInListTransformation = false;

	private DbmsPrinter out;
	HashMap<String, Table> tableNameToTableMap; 
	private ArrayList<Table> tables;

	public DBMS() {
		tableNameToTableMap = new HashMap<String, Table>();
		tables = new ArrayList<Table>();
	}

	/**
	 * Main method to run the DBMS engine.
	 * 
	 * @param args
	 *            arg[0] is input file, arg[1] is output file.
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		DBMS db = new DBMS();
		db.out = new DbmsPrinter();
		Scanner in = null;
		try {
			// set input file
			if (args.length > 0) {
				in = new Scanner(new File(args[0]));
			} else {  
				in = new Scanner(new File(COMMAND_FILE_LOC));
			}

			// set output files
			if (args.length > 1) {
				db.out.addPrinter(args[1]);
			} else {
				db.out.addPrinter(OUTPUT_FILE_LOC);
			}

			// Load data to memory
			db.loadTables();

			// Go through each line in the Command.txt file
			while (in.hasNextLine()) {
				String sql = in.nextLine();
				StringTokenizer tokenizer = new StringTokenizer(sql);

				// Evaluate the SQL statement
				if (tokenizer.hasMoreTokens()) {
					String command = tokenizer.nextToken();
					if (command.equalsIgnoreCase("CREATE")) {
						if (tokenizer.hasMoreTokens()) {
							command = tokenizer.nextToken();
							if (command.equalsIgnoreCase("TABLE")) {
								db.createTable(sql, tokenizer);
							} else if (command.equalsIgnoreCase("UNIQUE")) {
								if (tokenizer.hasMoreTokens() &&  "INDEX".equalsIgnoreCase(tokenizer.nextToken())) {
									db.createIndex(sql.toUpperCase(), tokenizer, true);
								}
							} else if (command.equalsIgnoreCase("INDEX")) {
								db.createIndex(sql.toUpperCase(), tokenizer, false);
							} else {
								throw new DbmsError("Invalid CREATE " + command
										+ " statement. '" + sql + "'.");
							}
						} else {
							throw new DbmsError("Invalid CREATE statement. '"
									+ sql + "'.");
						}
					} else if (command.equalsIgnoreCase("INSERT")) {
						db.insertInto(sql, tokenizer);
					} else if (command.equalsIgnoreCase("DROP")) {
						if (tokenizer.hasMoreTokens()) {
							command = tokenizer.nextToken();
							if (command.equalsIgnoreCase("TABLE")) {
								db.dropTable(sql, tokenizer);
							} else if (command.equalsIgnoreCase("INDEX")) {
								db.dropIndex(sql.toUpperCase(), tokenizer);
							} else {
								throw new DbmsError("Invalid DROP " + command
										+ " statement. '" + sql + "'.");
							}
						} else {
							throw new DbmsError("Invalid DROP statement. '"
									+ sql + "'.");
						}
					} else if (command.equalsIgnoreCase("RUNSTATS")) {
						String tableName = db.runstats(sql.toUpperCase(), tokenizer);
						db.printRunstats(tableName);
					} else if (command.equalsIgnoreCase("SELECT")) {
						db.select(sql, tokenizer);
					} else if (command.equalsIgnoreCase("--")) {
						// Ignore this command as a comment
					} else if (command.equalsIgnoreCase("COMMIT")) {
						try {
							// Check for ";"
							if (!tokenizer.nextElement().equals(";")) {
								throw new NoSuchElementException();
							}

							// Check if there are more tokens
							if (tokenizer.hasMoreTokens()) {
								throw new NoSuchElementException();
							}

							// Save tables to files
							for (Table table : db.tables) {
								db.storeTableFile(table);
							}
						} catch (NoSuchElementException ex) {
							throw new DbmsError("Invalid COMMIT statement. '"
									+ sql + "'.");
						}
					} else {
						throw new DbmsError("Invalid statement. '" + sql + "'.");
					}
				}
			}

			// Save tables to files
			for (Table table : db.tables) {
				db.storeTableFile(table);
			}
		} catch (DbmsError ex) {
			db.out.println("DBMS ERROR:  " + ex.getMessage());
			ex.printStackTrace();
		} catch (Exception ex) {
			db.out.println("JAVA ERROR:  " + ex.getMessage());
			ex.printStackTrace();
		} finally {
			// clean up
			try {
				in.close();
			} catch (Exception ex) {
			}

			try {
				db.out.cleanup();
			} catch (Exception ex) {
			}
		}
	}
	
	private void select(String sql, StringTokenizer tokenizer) throws DbmsError {
		if (!tokenizer.hasMoreTokens()) {
			throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
		}
		List<String> colNames = new ArrayList<String>();
		List<String> tableNames = new ArrayList<String>();
		String tok = tokenizer.nextToken();

		if (tok.equals("*")) {
			// TODO write the logic for it
		} else {
			if (!tokenizer.hasMoreTokens()) {
				throw new DbmsError("Invalid syntax for SELECT. FROM TableName missing. '" + sql + "'.");
			}

			while (!FROM.equalsIgnoreCase(tok)) {
				if (tok.contains(".") == false) {
					throw new DbmsError("Invalid syntax for SELECT. Column should be of the format TableName.ColumnName  '"	+ sql + "'.");
				}
				colNames.add(tok);
				tok = tokenizer.nextToken();
				if (COMMA.equals(tok)) {
					tok = tokenizer.nextToken();
					if (FROM.equalsIgnoreCase(tok) || tok.contains(".") == false) {
						throw new DbmsError("Invalid syntax for SELECT. FROM TableName missing. '" + sql + "'.");
					}
					continue;
				} else if (FROM.equalsIgnoreCase(tok)) {
					continue;
				} else {
					throw new DbmsError("Invalid syntax for SELECT. FROM TableName missing. '" + sql + "'.");
				}
			}

		}

		//TODO Handle in case of *
		if(colNames == null || colNames.isEmpty() || colNames.size() < 1){
			throw new DbmsError("Invalid syntax for SELECT. Atleast One Column Required '" + sql + "'.");
		}

		if (!tokenizer.hasMoreTokens() && FROM.equalsIgnoreCase(tok) == false) {
			throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
		}

		//After FROM
		tok = tokenizer.nextToken();
		tableNames.add(tok);

		if (!tokenizer.hasMoreTokens()) {
			semanticCheck(tableNames, colNames, null, null, sql);
			return;
		} else {
			tok = tokenizer.nextToken();
		}


		if(COMMA.equals(tok)) {
			if (!tokenizer.hasMoreElements() ) {
				throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
			}
			tok = tokenizer.nextToken();
			if (WHERE.equalsIgnoreCase(tok) || ORDER.equalsIgnoreCase(tok) ) {
				throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
			}
			while(!WHERE.equalsIgnoreCase(tok) && !ORDER.equalsIgnoreCase(tok) /*&& !SEMI_COLON.equals(tok)*/ ){
				tableNames.add(tok);
				if (!tokenizer.hasMoreTokens()) {
					throw new DbmsError("Invalid syntax for SELECT. ; missing '" + sql + "'.");
				}

				tok = tokenizer.nextToken();
				if(COMMA.equals(tok)) {
					tok = tokenizer.nextToken();
					continue;
				} else if(WHERE.equalsIgnoreCase(tok) || ORDER.equalsIgnoreCase(tok) /*|| SEMI_COLON.equals(tok)*/) {
					continue ;
				} else {
					throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
				}
			}
		}  

		if (tableNames == null || tableNames.isEmpty() == true || tableNames.size() < 1) {
			throw new DbmsError("Invalid syntax for SELECT. Atleast one Table Required. '" + sql + "'.");
		}


		List<String> predicates = new ArrayList<String>();
		if (WHERE.equalsIgnoreCase(tok)) {
			if (!tokenizer.hasMoreTokens()) {
				throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
			}
			tok = tokenizer.nextToken();
			while(tokenizer.hasMoreTokens() == true && ORDER.equalsIgnoreCase(tok) == false /*&& !SEMI_COLON.equals(tok)*/){
				StringBuilder predicateBuilder = new StringBuilder();
				while(!AND.equalsIgnoreCase(tok) && !OR.equalsIgnoreCase(tok) && !ORDER.equalsIgnoreCase(tok)  && tokenizer.hasMoreTokens()) {
					predicateBuilder.append(tok).append(" ");
					if (IN.equalsIgnoreCase(tok)) {
						tok = tokenizer.nextToken();
						if (!OPENING_BRACE.equals(tok)) {
							throw new DbmsError("Invalid syntax for SELECT. '(' missing '" + sql + "'.");
						}
						if (!tokenizer.hasMoreTokens()) {
							throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
						}
						while(!CLOSING_BRACE.equals(tok)){
							predicateBuilder.append(tok).append(" ");
							tok = tokenizer.nextToken();
							if (CLOSING_BRACE.equals(tok)) {
								predicateBuilder.append(tok).append(" ");
								continue;
							}else if (COMMA.equals(tok)) {
								tok = tokenizer.nextToken();
							} 

						}
						if (CLOSING_BRACE.equals(tok)) {
							if (tokenizer.hasMoreTokens()) {
								tok = tokenizer.nextToken();
							}else{
								break;
							}
						} else {
							throw new DbmsError("Invalid syntax for SELECT. ')' missing '" + sql + "'."); 
						}

					} else{
						if (tokenizer.hasMoreTokens()) {
							tok = tokenizer.nextToken();
						} else {
							predicates.add(predicateBuilder.toString().trim());
							break;
						}
					}
				}


				if (AND.equalsIgnoreCase(tok) || OR.equalsIgnoreCase(tok)) {
					predicates.add(predicateBuilder.toString().trim());
					predicates.add(tok);
					if (!tokenizer.hasMoreTokens()) {
						throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
					}
					tok = tokenizer.nextToken();
				} else if (ORDER.equalsIgnoreCase(tok) ) {
					predicates.add(predicateBuilder.toString().trim());
					continue;
				} else if (tokenizer.hasMoreTokens() == false) {
					predicateBuilder.append(tok);
					predicates.add(predicateBuilder.toString().trim());
					continue;
				} else {
					throw new DbmsError("Invalid syntax for SELECT. ';' missing '" + sql + "'.");
				}

			}

		} 

		List<String> orderByColumns = new ArrayList<String>();
		if (ORDER.equalsIgnoreCase(tok)) {
			if (!tokenizer.hasMoreTokens() || !tokenizer.nextToken().equalsIgnoreCase("BY")) {
				throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
			}

			if (!tokenizer.hasMoreTokens()) {
				throw new DbmsError("Invalid syntax for SELECT. '" + sql + "'.");
			}

			while(tokenizer.hasMoreTokens()){
				tok = tokenizer.nextToken();
				StringBuilder orderByColsBuilder = new StringBuilder();
				if (!"D".equalsIgnoreCase(tok) && tok.contains(".") == false) {
					throw new DbmsError("Invalid syntax for SELECT. Column should be of the format TableName.ColumnName  '"	+ sql + "'.");
				}

				orderByColsBuilder.append(tok);

				if (!tokenizer.hasMoreTokens()) {
					orderByColsBuilder.append(" ").append("A");
					orderByColumns.add(orderByColsBuilder.toString());
					break;
				}

				tok = tokenizer.nextToken();
				if (COMMA.equals(tok)) {
					orderByColsBuilder.append(" ").append("A");
					if (!tokenizer.hasMoreElements() ) {
						throw new DbmsError("Invalid syntax for SELECT.'" + sql + "'.");
					}
				} else if ("D".equalsIgnoreCase(tok)) {
					orderByColsBuilder.append(" ").append("D");
					if (tokenizer.hasMoreElements()) {
						tok = tokenizer.nextToken();
					} else {
						orderByColumns.add(orderByColsBuilder.toString());
						continue;
					} 
					if (COMMA.equals(tok)) {
						//	tok = tokenizer.nextToken();
					} else if (tokenizer.hasMoreElements() == false) {
						orderByColumns.add(orderByColsBuilder.toString());
						continue;
					} else {
						throw new DbmsError("Invalid syntax for SELECT.'" + sql + "'.");
					}
				} else {
					throw new DbmsError("Invalid syntax for SELECT.'" + sql + "'.");
				}

				orderByColumns.add(orderByColsBuilder.toString());
			}
		} 

		semanticCheck(tableNames, colNames, predicates, orderByColumns, sql);
	}


	private void semanticCheck(List<String> selectTableNames, List<String> selectColumnNames,
			List<String> selectPredicates, List<String> orderByColumns, String sql) throws DbmsError {

		out.println(sql);
		for (String tableName : selectTableNames) {
			if (!isTable(tableName)) {
				throw new DbmsError("Invalid syntax for SELECT. Reason: Table does not exist. '" + sql + "'.");
			}
			IndexList indexList = new IndexList(getTable(tableName).getIndexes());
			indexList.printTable(out);
			calculateRunstats(tableName);
		}

		Map<String, List<Column>> tableNameToSelectColumnsMap = new HashMap<String, List<Column>>();

		for (String selectColumn : selectColumnNames) {
			Column column = getColumn(selectColumn);
			if (column == null) {
				throw new DbmsError("Invalid syntax for SELECT. Invalid select column " + selectColumn + ". '" + sql + "'." );
			}

			String tableName = getTableNameFromPredicate(selectColumn);
			if (tableNameToSelectColumnsMap.containsKey(tableName)) {
				tableNameToSelectColumnsMap.get(tableName).add(column);
			} else {
				List<Column> selectColumns = new ArrayList<Column>();
				selectColumns.add(column);
				tableNameToSelectColumnsMap.put(tableName, selectColumns);
			}
		}


		Map<String, List<String>> tableNameToOrderByColumnIdsMap = new HashMap<String, List<String>>();
		Map<String, List<Column>> tableNameToOrderByColumnsMap = new HashMap<String, List<Column>>();
		if (orderByColumns != null && orderByColumns.isEmpty() == false) {
			for (String orderByColumn : orderByColumns) {
				String val[] = orderByColumn.split(" ");
				String order = val[1];
				Column column = getColumn(val[0]);
				if (column == null) {
					throw new DbmsError("Invalid syntax for SELECT. Invalid orderBy column " + orderByColumn + ". '" + sql + "'." );
				} else {
					String tableName = getTableNameFromPredicate(orderByColumn);
					int colId = column.getColId();
					String value = String.valueOf(colId) + " " + val[1];
					if (tableNameToOrderByColumnIdsMap.containsKey(tableName)) {
						tableNameToOrderByColumnIdsMap.get(tableName).add(value);
					} else {
						List<String> orderByCols = new ArrayList<String>();
						orderByCols.add(value);
						tableNameToOrderByColumnIdsMap.put(tableName, orderByCols);
					}

					if (tableNameToOrderByColumnsMap.containsKey(tableName)) {
						tableNameToOrderByColumnsMap.get(tableName).add(column);
					} else {
						List<Column> orderByCols = new ArrayList<Column>();
						orderByCols.add(column);
						tableNameToOrderByColumnsMap.put(tableName, orderByCols);
					}
				}
			}
		}


		Map<String, ArrayList<Predicate>> tableToPredicatesMap = new HashMap<String, ArrayList<Predicate>>();		
		List<Predicate> predicates = new ArrayList<Predicate>();
		List<Predicate> allPredicates = new ArrayList<Predicate>();

		if (selectPredicates != null && selectPredicates.isEmpty() == false) {
			for(int j = 0 ; j < selectPredicates.size() ; j ++) {
				String selectPredicate = selectPredicates.get(j);
				Predicate predicate;
				if (OR.equalsIgnoreCase(selectPredicate)) {
					continue;
				} else if (AND.equalsIgnoreCase(selectPredicate)) {
					j++;
					predicate = processPredicate(selectPredicates.get(j), tableToPredicatesMap);
					int predicateListSize = predicates.size();
					Predicate andPredicate = predicates.get(predicateListSize-1);
					while(andPredicate.getPredicate() != null){
						andPredicate = andPredicate.getPredicate();
					}
					andPredicate.setPredicate(predicate);
				} else{
					predicate = processPredicate(selectPredicates.get(j) , tableToPredicatesMap);
					predicates.add(predicate);
				}
				allPredicates.add(predicate);
			}
		}

		evaluatePredicateList(predicates);
		reverseInListTransformation(allPredicates);
		inListTrasformation(allPredicates, predicates, tableToPredicatesMap);
		errorTesting(predicates, allPredicates, tableToPredicatesMap);

		Map<String, Map<String, List<Predicate>>> indexToPhaseToPredicatesMap = new HashMap<String, Map<String,List<Predicate>>>();
		if (evaluateIndex) {
			indexToPhaseToPredicatesMap = indexWisePhaseCalculation(selectTableNames , tableToPredicatesMap);
		}

		PlanTable planTable = new PlanTable();

		if (selectTableNames.size() == 1) {
			String tableName = selectTableNames.get(0);
			singleTableAccess(tableName, planTable, predicates, allPredicates, 
					tableNameToSelectColumnsMap, tableNameToOrderByColumnsMap, tableNameToOrderByColumnIdsMap, 
					indexToPhaseToPredicatesMap);
		} else {
			predicateTransitiveClosure(selectTableNames,predicates, allPredicates, tableToPredicatesMap );
			multipleTableAccess(selectTableNames, planTable, predicates, allPredicates, tableToPredicatesMap, 
					tableNameToSelectColumnsMap, tableNameToOrderByColumnsMap, tableNameToOrderByColumnIdsMap, 
					indexToPhaseToPredicatesMap);
		}
		
		planTable.printTable(out);
		Predicate.printTable(out, (ArrayList<Predicate>) allPredicates);
	}


	
	/**
	 * This method will check for all sorts of errors in predicates and errorneous predicates sequence will be set to 0.
	 * @param predicates
	 * @param allPredicates
	 * @param tableToPredicatesMap
	 */
	private void errorTesting(List<Predicate> predicates,
			List<Predicate> allPredicates,
			Map<String, ArrayList<Predicate>> tableToPredicatesMap) {

		for (Predicate predicate : predicates) {
			testForLiteralPredicates(predicate);
		}

		if (predicates.size() == 1) {
			for (Entry<String, ArrayList<Predicate>> entry: tableToPredicatesMap.entrySet()) {
				Map<Integer, List<Predicate>> colIdToPredicatesMap = new HashMap<Integer, List<Predicate>>();
				String tableName = entry.getKey();
				List<Predicate> preds = entry.getValue();
				for (int i =0 ; i< preds.size() ; i++) {
					if (preds.get(i).isJoin()) {
						continue;
					}
					
					checkForRangeError(preds.get(i), tableName);
					if (colIdToPredicatesMap.containsKey(preds.get(i).getCol1Id())) {
						continue;
					} 

					for (int j = i + 1; j < preds.size(); j++) {
						if (preds.get(i).getCol1Id() == preds.get(j).getCol1Id()) {
							if (colIdToPredicatesMap.containsKey(preds.get(i).getCol1Id())) {
								colIdToPredicatesMap.get(preds.get(i).getCol1Id()).add(preds.get(j));
							} else {
								List<Predicate> predsInCol = new ArrayList<Predicate>();
								predsInCol.add(preds.get(i));
								predsInCol.add(preds.get(j));
								colIdToPredicatesMap.put(preds.get(i).getCol1Id(), predsInCol);
							} 
						}
					}

					for (Entry<Integer, List<Predicate>> entry1 : colIdToPredicatesMap.entrySet()) {
						if (entry1.getValue().size() > 1) {
							int colId = entry1.getKey();
							Column column = getTable(tableName).getColumnFromId(colId);
							for (int m = 0; m < entry1.getValue().size(); m++ ) {
								for (int n = m + 1; n < entry1.getValue().size(); n++) {
									Predicate pred1 = entry1.getValue().get(m);
									Predicate pred2 = entry1.getValue().get(n);
									String rhs1 = getRhsOfPredicate(pred1);
									String rhs2 = getRhsOfPredicate(pred2);


									//4 cases = =, r r , = r, r e
									if (pred1.getType() == Predicate.Type.E.getVal() && pred2.getType() == Predicate.Type.E.getVal()) {
										if (!rhs1.equalsIgnoreCase(rhs2)) {
											pred1.setSequence(0);
											pred2.setSequence(0);
										} 
									}

									if (pred1.getType() == Predicate.Type.R.getVal() && pred2.getType() == Predicate.Type.R.getVal()) {
										String relOp1 = getRelOp(pred1);
										String relOp2 = getRelOp(pred2);
										//Case of < >
										if (!relOp1.equalsIgnoreCase(relOp2)) {

											String greaterThanValue;
											String lessThanValue;
											if (relOp1.equals(">")) {
												greaterThanValue = rhs1;
												lessThanValue = rhs2;
											} else {
												greaterThanValue = rhs2;
												lessThanValue = rhs1;
											}


											if (Column.ColType.INT == column.getColType()) {
												Integer rhsVal1 = Integer.parseInt(greaterThanValue);
												Integer rhsVal2 = Integer.parseInt(lessThanValue);
												if (rhsVal1 == rhsVal2) {
													//Case when c1 < 10 and c1 > 10
													pred1.setSequence(0);
													pred2.setSequence(0);
												} else if (rhsVal1 > rhsVal2) {
													//Case when c1 > 10 and c1 < 5
													pred1.setSequence(0);
													pred2.setSequence(0);
												} 
											} else {
												if (rhs1.equalsIgnoreCase(rhs2)) {
													//Case when c1 < 10 and c1 > 10
													pred1.setSequence(0);
													pred2.setSequence(0);
												} else if (greaterThanValue.compareToIgnoreCase(lessThanValue) == 1) {
													//Case when c1 > 10 and c1 < 5
													pred1.setSequence(0);
													pred2.setSequence(0);
												}
											}
										} 
									}

									if (pred1.getType() == Predicate.Type.E.getVal() && pred2.getType() == Predicate.Type.R.getVal() ||
											pred1.getType() == Predicate.Type.R.getVal() && pred2.getType() == Predicate.Type.E.getVal()) {
										String relOp1 = getRelOp(pred1);
										String relOp2 = getRelOp(pred2);

										String equalPredicate;
										String rangePredicate;
										String range;
										if (relOp1.equals("=")) {
											equalPredicate = rhs1;
											rangePredicate = rhs2;
											range = relOp2;
										} else {
											equalPredicate = rhs2;
											rangePredicate = rhs1;
											range = relOp1;
										}

										if (Column.ColType.INT == column.getColType()) {
											Integer equalPredVal = Integer.parseInt(equalPredicate);
											Integer rangePredVal = Integer.parseInt(rangePredicate);

											if (range.equals(">")) {
												//Case when c1 = 10 and c1 > 10 || c1 = 10 and c1 > 15
												if (rangePredVal >= equalPredVal) {
													pred1.setSequence(0);
													pred2.setSequence(0);
												}
											}

											if (range.equals("<")) {
												//Case when c1 = 10 and c1 < 10 || c1 = 10 and c1 < 5
												if (rangePredVal <= equalPredVal) {
													pred1.setSequence(0);
													pred2.setSequence(0);
												}
											}
										} else {
											if (range.equals(">")) {
												//Case when c1 = 10 and c1 > 10 || c1 = 10 and c1 > 15
												if (rangePredicate.compareToIgnoreCase(equalPredicate) >= 0) {
													pred1.setSequence(0);
													pred2.setSequence(0);
												}
											}

											if (range.equals("<")) {
												//Case when c1 = 10 and c1 < 10 || c1 = 10 and c1 < 5
												if (rangePredicate.compareToIgnoreCase(equalPredicate) <= 0) {
													pred1.setSequence(0);
													pred2.setSequence(0);
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}

			boolean error = false;
			//Since the preds are in a combination of And, even if one element has sequence 0. All elements will be set to sequence 0 
			for (Predicate predicate : allPredicates) {
				if (predicate.getSequence() == 0 && predicate.getFf1() != 1) {
					error = true;
					break;
				}
			}

			if (error) {
				for (Predicate predicate : allPredicates) {
					predicate.setSequence(0);
				}	
			}
		} else {

			//Check if all Or's
			boolean allOrs = true;
			for (Predicate predicate : predicates) {
				if (predicate.getPredicate() != null) {
					allOrs = false;
				}
			}

			if (allOrs) {
				for (Entry<String, ArrayList<Predicate>> entry: tableToPredicatesMap.entrySet()) {
					Map<Integer, List<Predicate>> colIdToPredicatesMap = new HashMap<Integer, List<Predicate>>();
					String tableName = entry.getKey();
					List<Predicate> preds = entry.getValue();
					for (int i =0 ; i< preds.size() ; i++) {
						checkForRangeError(preds.get(i), tableName);
						if (colIdToPredicatesMap.containsKey(preds.get(i).getCol1Id())) {
							continue;
						} 

						for (int j = i + 1; j < preds.size(); j++) {
							if (preds.get(i).getCol1Id() == preds.get(j).getCol1Id()) {
								if (colIdToPredicatesMap.containsKey(preds.get(i).getCol1Id())) {
									colIdToPredicatesMap.get(preds.get(i).getCol1Id()).add(preds.get(j));
								} else {
									List<Predicate> predsInCol = new ArrayList<Predicate>();
									predsInCol.add(preds.get(i));
									predsInCol.add(preds.get(j));
									colIdToPredicatesMap.put(preds.get(i).getCol1Id(), predsInCol);
								} 
							}
						}

					}
					for (Entry<Integer, List<Predicate>> entry1 : colIdToPredicatesMap.entrySet()) {
						if (entry1.getValue().size() > 1) {
							int colId = entry1.getKey();
							Column column = getTable(tableName).getColumnFromId(colId);
							for (int m = 0; m < entry1.getValue().size(); m++ ) {
								for (int n = m + 1; n < entry1.getValue().size(); n++) {
									Predicate pred1 = entry1.getValue().get(m);
									Predicate pred2 = entry1.getValue().get(n);
									String rhs1 = getRhsOfPredicate(pred1);
									String rhs2 = getRhsOfPredicate(pred2);

									if (pred1.getType() == Predicate.Type.R.getVal() && pred2.getType() == Predicate.Type.R.getVal()) {
										String relOp1 = getRelOp(pred1);
										String relOp2 = getRelOp(pred2);

										if (relOp1.equalsIgnoreCase(relOp2)) {
											if (relOp1.equals(">")) {
												if (Column.ColType.INT == column.getColType()) {
													int rhs1Value = Integer.parseInt(rhs1);
													int rhs2Value = Integer.parseInt(rhs2);
													if (rhs1Value >= rhs2Value) {
														pred1.setSequence(0);
													} else {
														pred2.setSequence(0);
													}
												} else {
													if (rhs1.compareToIgnoreCase(rhs2) >= 0) {
														pred1.setSequence(0);
													} else {
														pred2.setSequence(0);
													}
												}
											} else {
												if (Column.ColType.INT == column.getColType()) {
													int rhs1Value = Integer.parseInt(rhs1);
													int rhs2Value = Integer.parseInt(rhs2);
													if (rhs1Value <= rhs2Value) {
														pred1.setSequence(0);
													} else {
														pred2.setSequence(0);
													}
												} else {
													if (rhs1.compareToIgnoreCase(rhs2) <= 0) {
														pred1.setSequence(0);
													} else {
														pred2.setSequence(0);
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}		
		}
	}

	
	/**
	 * Checks for range error, and sets the sequence of erroneous predicates to 0.
	 * Eg. c1 > 100 where hikey = 50 will never be true. 
	 * @param predicate
	 * @param tableName
	 */
	private void checkForRangeError(Predicate predicate, String tableName) {

		if (predicate.isJoin() || predicate.inList) {
			return;
		}
		
		Table table = getTable(tableName);
		Column column = table.getColumnFromId(predicate.getCol1Id());
		String rhsValue = getRhsOfPredicate(predicate);
		if (predicate.getType() == Predicate.Type.R.getVal()) {
			String relOp = getRelOp(predicate);
			if (relOp.equals(">")) {
				if (Column.ColType.INT == column.getColType()) {
					int rhsInt = Integer.parseInt(rhsValue);
					int highKey = Integer.parseInt(column.getHiKey());
					if (rhsInt >= highKey) {
						predicate.setSequence(0);
						predicate.setFf1(0);
					}
				} else {
					int comparator = rhsValue.compareToIgnoreCase(column.getHiKey());
					if (comparator >= 0) {
						predicate.setSequence(0);
						predicate.setFf1(0);
					}
				}

			} else {
				if (Column.ColType.INT == column.getColType()) {
					int rhsInt = Integer.parseInt(rhsValue);
					int lowKey = Integer.parseInt(column.getLoKey());
					if (rhsInt <= lowKey) {
						predicate.setSequence(0);
						predicate.setFf1(0);
					}
				} else {
					int comparator = rhsValue.compareToIgnoreCase(column.getLoKey());
					if (comparator <= 0) {
						predicate.setSequence(0);
						predicate.setFf1(0);
					}

				}
			}

		}

		if (predicate.getType() == Predicate.Type.E.getVal()) {
			if (Column.ColType.INT == column.getColType()) {
				int rhsInt = Integer.parseInt(rhsValue);
				int colLowKey = Integer.parseInt(column.getLoKey());
				int colHighKey = Integer.parseInt(column.getHiKey());
				if (rhsInt < colLowKey || rhsInt > colHighKey) {
					predicate.setSequence(0);
					predicate.setFf1(0);
				}
			} else {
				if (rhsValue.compareToIgnoreCase(column.getLoKey()) < 0 || 
						rhsValue.compareToIgnoreCase(column.getHiKey()) > 0) {
					predicate.setSequence(0);
					predicate.setFf1(0);
				}
			}
		}

	}

	
	/**
	 * Checks weather the given literal predicates are errorneous or not.
	 * Eg 0 = 1 will be always False. So set FF to 0.
	 * @param predicate
	 */
	private void testForLiteralPredicates(Predicate predicate){
		boolean error = false;
		Predicate tempPredicate = new Predicate();
		tempPredicate = predicate;
		if (tempPredicate.getSequence() == 0 && tempPredicate.getFf1() == 0) {
			error = true;
		}
		while(tempPredicate.getPredicate() != null) {
			tempPredicate = tempPredicate.getPredicate();
			if (tempPredicate.getSequence() == 0 && tempPredicate.getFf1() == 0) {
				error = true;
			}
		}

		if (error == true) {
			predicate.setSequence(0);
			while(predicate.getPredicate() != null) {
				predicate = predicate.getPredicate();
				predicate.setSequence(0);
			}
		}
	}

	/**
	 * Applies Predicate Transitive Closure for Join Predicate.
	 * For Join Predicates, the PTC counterpart of the inner table will not be evaluated. And hence its sequence will be set to 0
	 * @param selectTableNames
	 * @param predicates
	 * @param allPredicates
	 * @param tableToPredicatesMap
	 * @throws DbmsError
	 */
	private void predicateTransitiveClosure(List<String> selectTableNames,
			List<Predicate> predicates, List<Predicate> allPredicates,
			Map<String, ArrayList<Predicate>> tableToPredicatesMap) throws DbmsError {

		if (predicates == null ||  predicates.isEmpty() == true) {
			return;
		}

		//Implies predicates have Or ==> Cannot apply PTC
		//Implies only 1 predicate so no PTC
		if (predicates.size() > 1 || allPredicates.size() == 1) {
			return;
		}

		Predicate joinPredicate = null;
		for (Predicate predicate : allPredicates) {
			if (predicate.isJoin() == true) {
				joinPredicate = predicate;
			}
		}

		if (joinPredicate == null) {
			return;
		}


		String lhs = getLhsOfPredicate(joinPredicate.getText());
		String lhsTableName = getTableNameFromPredicate(lhs);
		String rhs = getRhsOfPredicate(joinPredicate);
		String rhsTableName = getTableNameFromPredicate(rhs);
		List<Predicate> predicate1 = new ArrayList<Predicate>();
		List<Predicate> predicate2 = new ArrayList<Predicate>();

		List<Predicate> lhsTablePredicates = tableToPredicatesMap.get(lhsTableName);
		int lhsColId = joinPredicate.getCol1Id();
		for (Predicate predicate : lhsTablePredicates) {
			if (predicate.isJoin()) {
				continue;
			}

			if (Predicate.Type.I.getVal() == predicate.getType()) {
				continue;
			}

			if (predicate.getCol1Id() == lhsColId) {
				predicate1.add(predicate);
			}
		}


		List<Predicate> rhsTablePredicates = tableToPredicatesMap.get(rhsTableName);
		int rhsColId = joinPredicate.getCol2Id();
		for (Predicate predicate : rhsTablePredicates) {
			if (predicate.isJoin()) {
				continue;
			}

			if (Predicate.Type.I.getVal() == predicate.getType()) {
				continue;
			}

			if (predicate.getCol1Id() == rhsColId) {
				predicate2.add(predicate);
			}
		}

		if(predicate1.isEmpty() == true && predicate2.isEmpty() == true) {
			//Both null no PTC
			return;
		}

		if (predicate1.isEmpty() == false || predicate2.isEmpty() == false) {
			if (predicate1.isEmpty() == false) {
				for (Predicate pred1 : predicate1) {

					String relOp = getRelOp(pred1);

					boolean constructPredicate = true;
					if (predicate2.isEmpty() == false ) {
						for (Predicate rhsPredicate : predicate2) {
							String rhsRelOp = getRelOp(rhsPredicate);
							//Ptc predicate for pred1 already exists
							if (relOp.equalsIgnoreCase(rhsRelOp)) {
								//TODO set as ptc counterparts
								rhsPredicate.setPtcPredicate(pred1);
								pred1.setPtcPredicate(rhsPredicate);
								constructPredicate = false;
							}
						}
					}
					if (constructPredicate == false) {
						continue;
					}	

					Predicate ptcPredicate = new Predicate();	
					//Add Predicate 2 in RHS Table
					ptcPredicate.setDescription("PTC");
					String rhsValue = getRhsOfPredicate(pred1);


					ptcPredicate.setText(rhs + " " + relOp + " " + rhsValue);
					ptcPredicate.setCol1Id(rhsColId);
					ptcPredicate.setCard1(joinPredicate.getCard2());

					Column column = getTable(rhsTableName).getColumnFromId(rhsColId);
					double ff1 = calculateFF(column, relOp, rhsValue);
					ptcPredicate.setFf1(ff1);

					ptcPredicate.setInList(false);
					ptcPredicate.setType(pred1.getType());

					ptcPredicate.setPtcPredicate(pred1);
					pred1.setPtcPredicate(ptcPredicate);

					allPredicates.add(ptcPredicate);
					tableToPredicatesMap.get(rhsTableName).add(ptcPredicate);

					Predicate predicate = joinPredicate.getPredicate();
					while(predicate.getPredicate() != null){
						predicate = predicate.getPredicate();
					}

					predicate.setPredicate(ptcPredicate);

				}
			}
			if (predicate2.isEmpty() == false) {
				//Add Predicate 1 in LHS Table
				for (Predicate pred2 : predicate2) {

					String relOp = getRelOp(pred2);
					boolean constructPredicate = true;
					if (predicate1.isEmpty() == false ) {
						for (Predicate lhsPredicate : predicate1) {
							String lhsRelOp = getRelOp(lhsPredicate);
							//Ptc predicate for pred1 already exists
							if (relOp.equalsIgnoreCase(lhsRelOp)) {
								//TODO set as ptc counterparts
								lhsPredicate.setPtcPredicate(pred2);
								pred2.setPtcPredicate(lhsPredicate);
								constructPredicate = false;
							}
						}
					}
					if (constructPredicate == false) {
						continue;
					}	


					Predicate ptcPredicate = new Predicate();
					ptcPredicate.setDescription("PTC");
					String rhsValue = getRhsOfPredicate(pred2);

					Column column = getTable(lhsTableName).getColumnFromId(lhsColId);
					double ff1 = calculateFF(column, relOp, rhsValue);
					ptcPredicate.setFf1(ff1);

					ptcPredicate.setText(lhs + " " + relOp + " " + rhsValue);
					ptcPredicate.setCol1Id(lhsColId);
					ptcPredicate.setCard1(joinPredicate.getCard1());
					ptcPredicate.setInList(false);
					ptcPredicate.setType(pred2.getType());

					ptcPredicate.setPtcPredicate(pred2);
					pred2.setPtcPredicate(ptcPredicate);

					allPredicates.add(ptcPredicate);
					tableToPredicatesMap.get(lhsTableName).add(ptcPredicate);

					Predicate predicate = joinPredicate.getPredicate();
					while(predicate.getPredicate() != null){
						predicate = predicate.getPredicate();
					}

					predicate.setPredicate(ptcPredicate);
				}
			}
		} 
	}

	private String getRelOp(Predicate predicate1) {
		String val[] = predicate1.getText().trim().split(" ");
		return val[1];

	}
	

	/**
	 * Flow for multiple table queries is set here.
	 * @param selectTableNames
	 * @param planTable
	 * @param predicates
	 * @param allPredicates
	 * @param tableToPredicatesMap
	 * @param tableNameToSelectColumnsMap
	 * @param tableNameToOrderByColumnsMap
	 * @param tableNameToOrderByColumnIdsMap
	 * @param indexToPhaseToPredicatesMap
	 * @throws DbmsError
	 */
	private void multipleTableAccess(
			List<String> selectTableNames,
			PlanTable planTable,
			List<Predicate> predicates,
			List<Predicate> allPredicates,
			Map<String, ArrayList<Predicate>> tableToPredicatesMap,
			Map<String, List<Column>> tableNameToSelectColumnsMap,
			Map<String, List<Column>> tableNameToOrderByColumnsMap,
			Map<String, List<String>> tableNameToOrderByColumnIdsMap,
			Map<String, Map<String, List<Predicate>>> indexToPhaseToPredicatesMap) throws DbmsError {

		int joinPredicateCount = 0;
		Predicate joinPredicate = new Predicate();
		for (Predicate predicate : allPredicates) {
			if (predicate.join == true) {
				joinPredicateCount++;
				joinPredicate = predicate;
			}
		}

		if (joinPredicateCount != 1) {
			throw new DbmsError("Exactly one join predicate allowed for a 2 table query.");
		}

		if (selectTableNames.size() == 2 ) {

			Table table1 =  getTable(selectTableNames.get(0));
			Table table2 = getTable(selectTableNames.get(1));

			int table1Card = table1.getTableCard();
			int table2Card = table2.getTableCard();

			planTable.setTable1Card(table1Card);
			planTable.setTable2Card(table2Card);


			List<Predicate> table1Predicates = tableToPredicatesMap.get(table1.getTableName());
			List<Predicate> table2Predicates = tableToPredicatesMap.get(table2.getTableName());

			Map<String, String> tableNameToSelectedIndexMap = indexSelectionForJoin(indexToPhaseToPredicatesMap, joinPredicate);

			//No selected index for join column 
			if (tableNameToSelectedIndexMap.get(table1.getTableName()) == null && tableNameToSelectedIndexMap.get(table2.getTableName()) == null) {
				//No local predicates exist
				planTable.setAccessType(PlanTable.AccessType.TABLESPACE_SCAN.getVal());
				planTable.setAccessName("");
				planTable.setIndexOnly('N');
				planTable.setPrefetch(PlanTable.Prefetch.SEQUENTIAL.getVal());
				if (table1Predicates.size() ==1 && table2Predicates.size() == 1) {

					if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
						planTable.setSortC_orderBy('Y');
					} else {
						planTable.setSortC_orderBy('N');
					}

					if (table1Card >= table2Card) {
						planTable.setLeadTable(table1.getTableName());
						orderJoinPredicates(table1, table2, tableToPredicatesMap);
					} else {
						planTable.setLeadTable(table2.getTableName());
						orderJoinPredicates(table1, table2, tableToPredicatesMap);
					}

				} else {
					//Local Predicates exist
					double table1FF = calculateFilterFactor(tableToPredicatesMap.get(table1.getTableName()), table1);
					double table2FF =  calculateFilterFactor(tableToPredicatesMap.get(table2.getTableName()), table2);

					if (table1FF >= table2FF) {
						planTable.setLeadTable(table1.getTableName());
						planTable.setInnerTable(table2.getTableName());
						orderJoinPredicates(table1, table2, tableToPredicatesMap);
					} else {
						planTable.setLeadTable(table2.getTableName());
						planTable.setInnerTable(table1.getTableName());
						orderJoinPredicates(table2, table1, tableToPredicatesMap);
					}
				}

				//TODO Handle Order By

			} else {
				//With Index
				//No local predicates exist

				planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());

				//TODO verify why prefetch is S with index scan 
				planTable.setPrefetch(PlanTable.Prefetch.SEQUENTIAL.getVal());


				if (tableNameToSelectedIndexMap != null && tableNameToSelectedIndexMap.isEmpty() == false) {

					String selectedIndexTable1 = tableNameToSelectedIndexMap.get(table1.getTableName());
					String selectedIndexTable2 = tableNameToSelectedIndexMap.get(table2.getTableName());


					if (selectedIndexTable1 != null && selectedIndexTable2 == null) {
						//Table1 becomes inner 
						planTable.setInnerTable(table1.getTableName());
						planTable.setLeadTable(table2.getTableName());

						planTable.setAccessName(table1.getTableName()+selectedIndexTable1);
						planTable.setIndexOnly('N');

						//If there is an order by clause on the inner table in the order of selected index then only sort will be avoided
						if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
							boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table1.getTableName(), selectedIndexTable1);
							if (sortAvoidance) {
								planTable.setSortC_orderBy('N');
							} else {
								planTable.setSortC_orderBy('Y');
							}
						}

						List<Predicate> matchingPredicates = indexToPhaseToPredicatesMap.get(table1.getTableName()+"."+selectedIndexTable1).get(Phase.MATCHING.getVal());
						List<Integer> colIds = new ArrayList<>();
						for (Predicate predicate : matchingPredicates) {
							if (!colIds.contains(predicate.getCol1Id())) {
								colIds.add(predicate.getCol1Id());
							}
						}

						planTable.setMatchCols(colIds.size());
						orderJoinPredicates(table2, table1, tableToPredicatesMap);

					} else if (selectedIndexTable2 != null && selectedIndexTable1 == null) {
						// Table 2 becomes inner
						planTable.setInnerTable(table2.getTableName());
						planTable.setLeadTable(table1.getTableName());

						planTable.setAccessName(table2.getTableName()+selectedIndexTable2);
						planTable.setIndexOnly('N');


						//If there is an order by clause on the inner table in the order of selected index then only sort will be avoided
						if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
							boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table2.getTableName(), selectedIndexTable2);
							if (sortAvoidance) {
								planTable.setSortC_orderBy('N');
							} else {
								planTable.setSortC_orderBy('Y');
							}
						}

						List<Predicate> matchingPredicates = indexToPhaseToPredicatesMap.get(table2.getTableName()+"."+selectedIndexTable2).get(Phase.MATCHING.getVal());
						planTable.setMatchCols(matchingPredicates.size());

						orderJoinPredicates(table1, table2, tableToPredicatesMap);


					} else if (selectedIndexTable1 != null && selectedIndexTable2 != null) {

						double table1FF = calculateFilterFactor(tableToPredicatesMap.get(table1.getTableName()), table1);
						double table2FF =  calculateFilterFactor(tableToPredicatesMap.get(table2.getTableName()), table2);

						//No Local Predicates
						if (table1Predicates.size() == 1 && table2Predicates.size() == 1) {

							planTable.setMatchCols(1);

							// Both have matching index chose a better one	

							if (table1FF < table2FF) {
								//Table 1 inner - Better FF inside
								planTable.setInnerTable(table1.getTableName());
								planTable.setLeadTable(table2.getTableName());

								planTable.setAccessName(table1.getTableName()+selectedIndexTable1);

								planTable.setIndexOnly('N');
								orderJoinPredicates(table2, table1, tableToPredicatesMap);

								if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
									boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table1.getTableName(), selectedIndexTable1);
									if (sortAvoidance) {
										planTable.setSortC_orderBy('N');
									} else {
										planTable.setSortC_orderBy('Y');
									}
								}

							} else if (table1FF > table2FF) {
								//Table 2 inner - Better FF inside
								planTable.setInnerTable(table2.getTableName());
								planTable.setLeadTable(table1.getTableName());

								planTable.setAccessName(table2.getTableName()+selectedIndexTable2);

								planTable.setIndexOnly('N');
								orderJoinPredicates(table1, table2, tableToPredicatesMap);

								//If there is an order by clause on the inner table in the order of selected index then only sort will be avoided
								if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
									boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table2.getTableName(), selectedIndexTable2);
									if (sortAvoidance) {
										planTable.setSortC_orderBy('N');
									} else {
										planTable.setSortC_orderBy('Y');
									}
								}


							} else {
								//Both equal FF	

								if (table1.getTableCard() >= table2.getTableCard()) {
									//Larger inner so Table 1 inner

									planTable.setInnerTable(table1.getTableName());
									planTable.setLeadTable(table2.getTableName());

									planTable.setAccessName(table1.getTableName()+selectedIndexTable1);
									planTable.setIndexOnly('N');
									orderJoinPredicates(table2, table1, tableToPredicatesMap);


									if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
										boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table1.getTableName(), selectedIndexTable1);
										if (sortAvoidance) {
											planTable.setSortC_orderBy('N');
										} else {
											planTable.setSortC_orderBy('Y');
										}
									}

								} else {
									//Larger inner so Table 2 inner
									planTable.setInnerTable(table2.getTableName());
									planTable.setLeadTable(table1.getTableName());

									planTable.setAccessName(table2.getTableName()+selectedIndexTable2);
									planTable.setIndexOnly('N');
									orderJoinPredicates(table1, table2, tableToPredicatesMap);

									//If there is an order by clause on the inner table in the order of selected index then only sort will be avoided
									if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
										boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table2.getTableName(), selectedIndexTable2);
										if (sortAvoidance) {
											planTable.setSortC_orderBy('N');
										} else {
											planTable.setSortC_orderBy('Y');
										}
									}

								}
							}
						} else {					

							if (table1FF <= table2FF) {
								//Table 1 inner - Better FF inside
								planTable.setInnerTable(table1.getTableName());
								planTable.setLeadTable(table2.getTableName());

								planTable.setAccessName(table1.getTableName()+selectedIndexTable1);

								planTable.setIndexOnly('N');
								orderJoinPredicates(table2, table1, tableToPredicatesMap);

								if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
									boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table1.getTableName(), selectedIndexTable1);
									if (sortAvoidance) {
										planTable.setSortC_orderBy('N');
									} else {
										planTable.setSortC_orderBy('Y');
									}
								}

								List<Predicate> matchingPredicates = indexToPhaseToPredicatesMap.get(table1.getTableName()+"."+selectedIndexTable1).get(Phase.MATCHING.getVal());
								planTable.setMatchCols(matchingPredicates.size());


							} else if (table1FF > table2FF) {
								//Table 2 inner - Better FF inside
								planTable.setInnerTable(table2.getTableName());
								planTable.setLeadTable(table1.getTableName());

								planTable.setAccessName(table2.getTableName()+selectedIndexTable2);

								planTable.setIndexOnly('N');
								orderJoinPredicates(table1, table2, tableToPredicatesMap);

								//If there is an order by clause on the inner table in the order of selected index then only sort will be avoided
								if (tableNameToOrderByColumnIdsMap != null && tableNameToOrderByColumnIdsMap.isEmpty() == false) {
									boolean sortAvoidance = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdsMap, table2.getTableName(), selectedIndexTable2);
									if (sortAvoidance) {
										planTable.setSortC_orderBy('N');
									} else {
										planTable.setSortC_orderBy('Y');
									}
								}

								List<Predicate> matchingPredicates = indexToPhaseToPredicatesMap.get(table2.getTableName()+"."+selectedIndexTable2).get(Phase.MATCHING.getVal());
								planTable.setMatchCols(matchingPredicates.size());

							} 
						}

					}
				}
			}
		}
	}


	/**
	 * Index Selection Logic for a query which has join predicates.
	 * For Join Index will be selected if only an index exists where the Join Column is the leading matching column
	 * @param indexToPhaseToPredicatesMap
	 * @param joinPredicate
	 * @return
	 */
	private Map<String, String> indexSelectionForJoin(
			Map<String, Map<String, List<Predicate>>> indexToPhaseToPredicatesMap,
			Predicate joinPredicate) {


		if (evaluateIndex == true) {

			if (indexToPhaseToPredicatesMap == null) {
				return null;
			}

			if (joinPredicate == null) {
				return null;
			}

			Map<String, String> tableNameToSelectedIndexMap = new HashMap<String, String>();

			for (Entry<String, Map<String, List<Predicate>>> entry : indexToPhaseToPredicatesMap.entrySet()) {
				int matchingMax = 0 , screeningMax = 0 ;
				String key[] = entry.getKey().trim().split("\\.");
				String tableName = key[0];
				String indexName = key[1];
				Map<String, List<Predicate>> phaseToPredicatesMap = entry.getValue();

				List<Predicate> matchingPredicates = phaseToPredicatesMap.get(Phase.MATCHING.getVal());
				if (matchingPredicates.contains(joinPredicate) == false) {
					continue;
				}

				Table table = getTable(tableName);
				Index index = table.getIndex(indexName);

				String joinColLhsTableName = getTableNameFromPredicate(getLhsOfPredicate(joinPredicate.getText()));
				int joinColId = 0;
				if (joinColLhsTableName.equalsIgnoreCase(tableName)) {
					joinColId = joinPredicate.getCol1Id();
				} else {
					joinColId = joinPredicate.getCol2Id();
				}

				for (Index.IndexKeyDef indexKeyDef : index.getIdxKey()) {
					//Implies Join Predicate isnt a leading Predicate
					if (indexKeyDef.colId == joinColId && indexKeyDef.idxColPos != 1) {
						return null;
					}
				}

				if (tableNameToSelectedIndexMap.containsKey(tableName)) {
					Map<String, List<Predicate>> currentSelection = indexToPhaseToPredicatesMap.get(tableName+"."+tableNameToSelectedIndexMap.get(tableName)); 
					matchingMax = currentSelection.get(Phase.MATCHING.val).size();
					screeningMax = currentSelection.get(Phase.SCREENING.val).size();
				}

				int matchingCount = phaseToPredicatesMap.get(Phase.MATCHING.getVal()).size();

				//The current entry is better so continue
				if (matchingCount < matchingMax) {
					continue;
				}

				//The this entry is better than current so update and continue
				if (matchingCount > matchingMax) {
					tableNameToSelectedIndexMap.put(tableName, indexName);
					continue;
				}

				//Both the entries are equal. So go to screening
				int screeningCount = phaseToPredicatesMap.get(Phase.SCREENING.val).size();

				if (screeningCount <  screeningMax) {
					continue;
				}

				if (screeningCount > screeningMax) {
					tableNameToSelectedIndexMap.put(tableName, indexName);
					continue;
				} 

				//Screening is also equal. So now go to Filter Factors.
				if (tableNameToSelectedIndexMap.containsKey(tableName)) {
					Map<String, List<Predicate>> currentSelection = indexToPhaseToPredicatesMap.get(tableName+"."+tableNameToSelectedIndexMap.get(tableName));
					double minFFCurrent = calculateLeastFilterFactor(currentSelection.get(Phase.MATCHING.val));
					double minFFThis = calculateLeastFilterFactor(phaseToPredicatesMap.get(Phase.MATCHING.val));

					//Min FF is better of the current value so continue
					if (minFFThis > minFFCurrent) {
						continue;
					} 

					//Min FF is better of this value so update and continue
					if (minFFThis < minFFCurrent) {
						tableNameToSelectedIndexMap.put(tableName, indexName);
						continue;
					}

					//Since none of the conditions satisfied minFF of both is equal. Go to minScreeningFF
					double minScreeningFFCurrent = calculateLeastFilterFactor(currentSelection.get(Phase.SCREENING.val));
					double minScreeningFFThis = calculateLeastFilterFactor(phaseToPredicatesMap.get(Phase.SCREENING.val));

					if (minScreeningFFThis > minScreeningFFCurrent) {
						continue;
					}

					if (minScreeningFFThis < minScreeningFFCurrent){
						tableNameToSelectedIndexMap.put(tableName, indexName);
						continue;
					}
					//Nothing satisfied at this point. Leave at this state
				} 
			}
			return tableNameToSelectedIndexMap;
		}
		return null;
	}

	
	
	/**
	 * Logic for ordering list of predicates which contains a join predicate.
	 * First the leading table local preds will be avaluated. Then the join column. Later the local preds of the inner table
	 * @param leadingTable
	 * @param innerTable
	 * @param tableToPredicatesMap
	 */
	private void orderJoinPredicates(Table leadingTable, Table innerTable, Map<String, ArrayList<Predicate>> tableToPredicatesMap) {

		List<Predicate> outerTablePredicates = tableToPredicatesMap.get(leadingTable.getTableName());

		int sequence = 1;
		List<Predicate> tempLeadingPredicates = new ArrayList<Predicate>();
		int joinPredicateIndex = 0;
		for (int i = 0; i < outerTablePredicates.size(); i++) {
			Predicate predicate = outerTablePredicates.get(i);
			if (predicate.isJoin()) {
				joinPredicateIndex = i;
				continue;
			}
			tempLeadingPredicates.add(predicate);
		}
		sequence = sortInAscendingAccordingToFF(tempLeadingPredicates, sequence);
		outerTablePredicates.get(joinPredicateIndex).setSequence(sequence);
		sequence++;

		for (Predicate predicate : outerTablePredicates) {
			Predicate ptcPredicate = predicate.getPtcPredicate();
			if (ptcPredicate != null) {
				ptcPredicate.setSequence(0);
			}
		}

		List<Predicate> innerTablePredicates = tableToPredicatesMap.get(innerTable.getTableName());
		List<Predicate> tempInnerPredicates = new ArrayList<Predicate>();
		for (int i = 0; i < innerTablePredicates.size(); i++) {
			Predicate predicate = innerTablePredicates.get(i);
			if (predicate.isJoin()) {
				continue;
			}
			tempInnerPredicates.add(predicate);
		}
		sequence = sortInAscendingAccordingToFF(tempInnerPredicates, sequence);

	}

	
	/**
	 * Filter Factor Calculation
	 * @param table1Predicates
	 * @param table
	 * @return
	 */
	private double calculateFilterFactor(List<Predicate> table1Predicates, Table table ) {

		double joinColFF = 0;
		double localPredicatesFF = 1;
		for (Predicate predicate : table1Predicates) {
			if (predicate.isJoin()) {
				//Verify what table we are calculating for
				String lhs = getLhsOfPredicate(predicate.getText());
				String lhsTableName = getTableNameFromPredicate(lhs);
				if (lhsTableName.equalsIgnoreCase(table.getTableName())) {
					joinColFF = predicate.getFf1();
				} else {
					joinColFF = predicate.getFf2();
				}
				continue;
			}

			localPredicatesFF *= predicate.getFf1();
		}

		double ff = localPredicatesFF * joinColFF * table.getTableCard();

		return ff;
	}

	
	
	/**
	 * Logic (acc to rules) for single table access 
	 * @param tableName
	 * @param planTable
	 * @param predicates
	 * @param allPredicates
	 * @param tableNameToSelectColumnsMap
	 * @param tableNameToOrderByColumnsMap
	 * @param tableNameToOrderByColumnIdMap
	 * @param indexToPhaseToPredicatesMap
	 */
	private void singleTableAccess(String tableName, PlanTable planTable,
			List<Predicate> predicates, List<Predicate> allPredicates , Map<String, List<Column>> tableNameToSelectColumnsMap,
			Map<String, List<Column>> tableNameToOrderByColumnsMap, Map<String, List<String>> tableNameToOrderByColumnIdMap,
			Map<String, Map<String, List<Predicate>>> indexToPhaseToPredicatesMap) {

		Table table = getTable(tableName);
		planTable.setTable1Card(table.getTableCard());

		//No index
		if (table.getIndexes() == null || table.getIndexes().isEmpty() == true || evaluateIndex == false) {
			planTable.setAccessType(PlanTable.AccessType.TABLESPACE_SCAN.getVal());
			planTable.setPrefetch(PlanTable.Prefetch.SEQUENTIAL.getVal());

			planTable.setMatchCols(0);
			planTable.setAccessName("");
			planTable.setIndexOnly('N');


			if (allPredicates != null && allPredicates.isEmpty() == false) {
				//Sequence the predicates
				orderPredicates(planTable, predicates, allPredicates);
			}
		} else {

			if (allPredicates == null || allPredicates.isEmpty() == true) {

				//Check for Index Only For select cols since no predicates exist

				Index selectedIndexForSelectCols = null;
				Index selectedIndexForOrderByCols = null;
				Index selectedIndex = null;

				for (Index index : table.getIndexes()) {
					boolean isIndexOnlyForSelectCols = checkSelectColsForIndexOnly(tableNameToSelectColumnsMap, tableName, index.getIdxName());
					boolean isIndexOnlyForOrderByCols = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdMap, tableName, index.getIdxName());
					if (isIndexOnlyForOrderByCols && isIndexOnlyForSelectCols) {
						selectedIndexForOrderByCols = index;
						selectedIndexForSelectCols = index;
					} else if (isIndexOnlyForOrderByCols && selectedIndexForOrderByCols == null) {
						selectedIndexForOrderByCols = index;
					} else if (isIndexOnlyForSelectCols && selectedIndexForSelectCols == null) {
						selectedIndexForSelectCols = index;
					} 

				}


				if (selectedIndexForOrderByCols != null && selectedIndexForSelectCols != null) {
					planTable.setAccessName(tableName+"."+ selectedIndexForOrderByCols.getIdxName());
					planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());
					planTable.setPrefetch(PlanTable.Prefetch.BLANK.getVal());
					planTable.setIndexOnly('Y');
					planTable.setSortC_orderBy('N');
				} else if (selectedIndexForOrderByCols != null) {
					planTable.setAccessName(tableName+"."+ selectedIndexForOrderByCols.getIdxName());
					planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());
					planTable.setPrefetch(PlanTable.Prefetch.BLANK.getVal());
					planTable.setIndexOnly('Y');
					planTable.setSortC_orderBy('N');
					planTable.setMatchCols(0);
				} else if (selectedIndexForSelectCols != null) {
					planTable.setAccessName(tableName+"."+ selectedIndexForSelectCols.getIdxName());
					planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());
					planTable.setPrefetch(PlanTable.Prefetch.BLANK.getVal());
					planTable.setIndexOnly('Y');
					planTable.setSortC_orderBy('N');
				} else {
					planTable.setAccessType(PlanTable.AccessType.TABLESPACE_SCAN.getVal());
					planTable.setPrefetch(PlanTable.Prefetch.SEQUENTIAL.getVal());
					planTable.setAccessName("");
					planTable.setIndexOnly('N');
					if (tableNameToOrderByColumnsMap.size() > 0) {
						planTable.setSortC_orderBy('Y');
					} else {
						planTable.setSortC_orderBy('N');
					}
				}
			} else {
				Map<String, String> tableNameToSelectedIndexMap = indexSelection(indexToPhaseToPredicatesMap);

				//Implies an index was selected for the given table
				if (tableNameToSelectedIndexMap != null && !tableNameToSelectedIndexMap.isEmpty()) {

					String selectedIndexName = tableNameToSelectedIndexMap.get(tableName);

					planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());
					planTable.setAccessName(tableName+"."+selectedIndexName);
					planTable.setPrefetch(PlanTable.Prefetch.BLANK.getVal());

					List<Predicate> matchingPredicates = indexToPhaseToPredicatesMap.get(tableName+"."+selectedIndexName).get(Phase.MATCHING.getVal());
					if (matchingPredicates != null && matchingPredicates.isEmpty() == false) {
						List<Integer> colIds = new ArrayList<>();
						for (Predicate predicate : matchingPredicates) {
							if (!colIds.contains(predicate.getCol1Id())) {
								colIds.add(predicate.getCol1Id());
							}
						}
						planTable.setMatchCols(colIds.size());
					} else {
						planTable.setMatchCols(0);
					}

					List<Predicate> screeningPredicates = indexToPhaseToPredicatesMap.get(tableName+"."+selectedIndexName).get(Phase.SCREENING.getVal());
					boolean isPredicatesIndexOnly = checkPredicatesForIndexOnly(matchingPredicates, screeningPredicates, allPredicates);

					if (isPredicatesIndexOnly) {
						boolean isIndexOnlyForSelectCols = checkSelectColsForIndexOnly(tableNameToSelectColumnsMap, tableName, selectedIndexName);
						if (isIndexOnlyForSelectCols) {
							planTable.setIndexOnly('Y');
						}else {
							planTable.setIndexOnly('N');
						}
					} else {
						planTable.setIndexOnly('N');
					}


					if (tableNameToOrderByColumnIdMap != null && tableNameToOrderByColumnIdMap.isEmpty() == false) {
						planTable.setSortC_orderBy('Y');
						boolean isIndexOnlyForOrderByCols = checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdMap, tableName, selectedIndexName);
						if (isIndexOnlyForOrderByCols) {
							planTable.setSortC_orderBy('N');
						}
					} else {
						planTable.setSortC_orderBy('N');
					}

					//TODO Check for InList Scan
					boolean isInListScan = isInListScan(matchingPredicates);
					if (isInListScan) {
						planTable.setAccessType(PlanTable.AccessType.IN_LIST_INDEX_SCAN.getVal());
					} 


				} else {

					planTable.setAccessType(PlanTable.AccessType.TABLESPACE_SCAN.getVal());
					planTable.setPrefetch(PlanTable.Prefetch.SEQUENTIAL.getVal());

					planTable.setAccessName("");
					planTable.setIndexOnly('N');

					if (tableNameToOrderByColumnIdMap != null && tableNameToOrderByColumnIdMap.isEmpty() == false) {
						//Check for orderby cols. If it is indexable then plan table type will change
						for (Index index : table.getIndexes()) {
							//Implies index can be used for order by
							if (checkForSortAvoidanceOrderBy(tableNameToOrderByColumnIdMap, tableName, index.getIdxName())) {
								planTable.setAccessType(PlanTable.AccessType.INDEX_SCAN.getVal());
								planTable.setIndexOnly('N');
								planTable.setMatchCols(0);
								planTable.setAccessName(tableName+"."+index.getIdxName());
								planTable.setPrefetch(PlanTable.Prefetch.BLANK.getVal());
								planTable.setSortC_orderBy('N');
							} 
						}
					} else {
						planTable.setSortC_orderBy('N');
					}
				}

				orderPredicates(planTable, predicates, allPredicates);

			}
		}
	}


	
	/**
	 * Checks for Index Only for a given Index for the list of predicates
	 * @param matchingPredicates
	 * @param screeningPredicates
	 * @param allPredicates
	 * @return
	 */
	private boolean checkPredicatesForIndexOnly(
			List<Predicate> matchingPredicates,
			List<Predicate> screeningPredicates, List<Predicate> allPredicates) {

		int matchingPredsLength = 0;
		if (matchingPredicates != null && matchingPredicates.isEmpty() == false) {
			matchingPredsLength = matchingPredicates.size();
		}

		int screeningPredsLength = 0;
		if (screeningPredicates != null && screeningPredicates.isEmpty() == false) {
			screeningPredsLength = screeningPredicates.size();
		}

		int indexablePredicatesLength = matchingPredsLength + screeningPredsLength;

		int allPredicatesLength = 0;
		if (allPredicates != null && allPredicates.isEmpty() == false) {
			allPredicatesLength = allPredicates.size();
		}

		if (indexablePredicatesLength == allPredicatesLength) {
			return true;
		}

		return false;
	}

	
	/***
	 * Checks for In List Index Access for the selected index matching predicates.
	 * @param matchingPredicates
	 * @return
	 */
	private boolean isInListScan(List<Predicate> matchingPredicates) {

		if (matchingPredicates == null || matchingPredicates.isEmpty() == true) {
			return false;
		}

		for (Predicate predicate : matchingPredicates) {
			if (predicate.isInList()) {
				return true;
			}
		}
		return false;
	}

	
	/**
	 * Checks for Sort Avoidance for the given index.
	 * @param tableNameToOrderByColumnIdMap
	 * @param tableName
	 * @param idxName
	 * @return
	 */
	private boolean checkForSortAvoidanceOrderBy(
			Map<String, List<String>> tableNameToOrderByColumnIdMap,
			String tableName, String idxName) {

		//Cannot avoid sort if order by on multiple tables
		if (tableNameToOrderByColumnIdMap.size() > 1) {
			return false;
		}

		List<String> orderByCols = tableNameToOrderByColumnIdMap.get(tableName);
		Table table = getTable(tableName);
		Index index = table.getIndex(idxName);

		boolean reverseOrder = false;

		int length = index.getIdxKey().size(); 
		if (orderByCols.size() < length) {
			length = orderByCols.size();
		}

		for (int i = 0 ; i < length; i++ ) {
			Index.IndexKeyDef indexKeyDef = index.getIdxKey().get(i);
			String indexOrder = "A";
			if (indexKeyDef.descOrder) {
				indexOrder = "D";
			}
			String val[] = orderByCols.get(i).trim().split(" ");
			int orderByColId = Integer.parseInt(val[0]);
			String orderByOrder = val[1];
			if (indexKeyDef.colId == orderByColId) {

				if (i == 0 && !orderByOrder.equalsIgnoreCase(indexOrder) ) {
					reverseOrder = true;
					continue;
				}

				if (reverseOrder) {
					if (orderByOrder.equalsIgnoreCase(indexOrder)) {
						return false;
					} 
				} else {
					if (!orderByOrder.equalsIgnoreCase(indexOrder)) {
						return false;
					}
				}

			} else {
				return false;
			}
		}
		return true;
	}

	
	/**
	 * Checks for Index Only for a given Index for select columns
	 * @param tableNameToSelectColumnsMap
	 * @param tableName
	 * @param indexName
	 * @return
	 */
	private boolean checkSelectColsForIndexOnly(Map<String, List<Column>> tableNameToSelectColumnsMap , String tableName, String indexName){

		Table table  = getTable(tableName);
		for (Column selectColumn : tableNameToSelectColumnsMap.get(tableName)) {
			/*						if (tableName.equalsIgnoreCase(getTableNameFromPredicate(selectColName)) == false) {
				continue;
			}
			 */
			List<Column> indexedColumns = table.getIndexedColumnsFromIndexName(indexName);
			if (indexedColumns.contains(selectColumn) == false) {
				return false;
			}
			//If none of the conditions above did not satisfy means it is Index Only
			return true;
		}
		return false;
	}
	

	/**
	 * Orders Predicates based on Index / Filter Factor
	 * @param planTable
	 * @param predicates
	 * @param allPredicates
	 */
	private void orderPredicates(PlanTable planTable, List<Predicate> predicates, List<Predicate> allPredicates) {
		if (PlanTable.AccessType.INDEX_SCAN.getVal() == planTable.getAccessType() || PlanTable.AccessType.IN_LIST_INDEX_SCAN.getVal() == planTable.getAccessType()) {
			//IScan will have a combination of Ands or single element so using All predicates.
			String accessName = planTable.getAccessName();
			String val[] = accessName.trim().split("\\.");
			String tableName = val[0];
			String indexName = val[1];
			Table table = getTable(tableName);
			Index index = table.getIndex(indexName);
			int sequence = 1;
			List<Predicate> nonIndexablePredicates = new ArrayList<Predicate>();
			for (Index.IndexKeyDef indexKeyDef : index.getIdxKey()) {
				for (Predicate predicate : allPredicates) {
					if (predicate.getSequence() == 0) {
						continue;
					}
					if (predicate.getCol1Id() == indexKeyDef.colId) {
						predicate.setSequence(sequence);
						sequence ++;
					} 
				}
			}


			//If Index Only then return else order non indexable predicates also
			if (planTable.getIndexOnly() == 'Y') {
				return;
			} else {
				for (Predicate predicate : allPredicates) {
					if (predicate.getSequence() == 999) {
						nonIndexablePredicates.add(predicate);
					}
				}
				sortInAscendingAccordingToFF(nonIndexablePredicates, sequence);

			}
		} else {

			//RScan will have a combination of Ands and Ors so using predicates.
			List<Predicate> predsToBeSquenced =  new ArrayList<Predicate>();
			int sequence = 1;
			for (Predicate predicate : predicates) {
				if (predicate.getSequence() == 0) {
					continue;
				}

				if (predicate.getPredicate() != null) {
					List<Predicate> andPredicates = new ArrayList<Predicate>();
					andPredicates.add(predicate);
					while (predicate.getPredicate() != null) {
						predicate = predicate.getPredicate();
						andPredicates.add(predicate);
					}
					sequence = sortInAscendingAccordingToFF(andPredicates, sequence);
				} else {
					predsToBeSquenced.add(predicate);
				}
			}
			sequence = sortInAscendingAccordingToFF(predsToBeSquenced, sequence);
		}
	}

	
	/**
	 * SOrts the given list of predicates based on Filter Factor
	 * @param nonIndexablePredicates
	 * @param sequence
	 * @return
	 */
	private int sortInAscendingAccordingToFF(
			List<Predicate> nonIndexablePredicates, int sequence) {

		Collections.sort(nonIndexablePredicates, new Comparator<Predicate>() {

			@Override
			public int compare(Predicate o1, Predicate o2) {
				Double ff1 =  o1.getFf1();
				Double ff2 =  o2.getFf1();
				return ff1.compareTo(ff2);

			}
		});


		Collections.sort(nonIndexablePredicates, new Comparator<Predicate>() {

			@Override
			public int compare(Predicate o1, Predicate o2) {
				return o1.getFf1() <= o2.getFf1() ? 0 : 1 ;
			}
		});

		for (Predicate predicate : nonIndexablePredicates) {
			if (predicate.getSequence() != 0) {
				predicate.setSequence(sequence);
				sequence ++;
			}
		}
		return sequence;

	}

	
	/**
	 * for the given list of predicates, it determines if Index is to be used or not and In List transformation is to be applied or not
	 * @param predicates
	 */
	
	private void evaluatePredicateList(List<Predicate> predicates) {
		if (predicates.size() > 1) {
			//Implies either a series of Or predicates or combi of And and Or

			for (Predicate predicate : predicates) {
				evaluateIndex = true;
				checkForInListTransformation = true;

				//Implies its a combination of Ands and Ors
				if (predicate.getPredicate() != null) {
					evaluateIndex = false;
					checkForInListTransformation = false;
					break;
				}
			}

		} else {
			//Implies list has 1 element only. So its either a combination of Ands or a single element
			evaluateIndex = true;
			checkForInListTransformation = false;
		}
	}

	
	/**
	 * Selects the strongest index 
	 * @param indexToPhaseToPredicatesMap
	 * @return
	 */
	private Map<String, String> indexSelection(Map<String, Map<String, List<Predicate>>> indexToPhaseToPredicatesMap) {

		if (indexToPhaseToPredicatesMap == null) {
			return null;
		}

		Map<String, String> tableNameToSelectedIndexMap = new HashMap<String, String>();

		for (Entry<String, Map<String, List<Predicate>>> entry : indexToPhaseToPredicatesMap.entrySet()) {
			int matchingMax = 0 , screeningMax = 0 ;
			String key[] = entry.getKey().trim().split("\\.");
			String tableName = key[0];
			String indexName = key[1];
			Map<String, List<Predicate>> phaseToPredicatesMap = entry.getValue();

			if (tableNameToSelectedIndexMap.containsKey(tableName)) {
				Map<String, List<Predicate>> currentSelection = indexToPhaseToPredicatesMap.get(tableName+"."+tableNameToSelectedIndexMap.get(tableName)); 
				matchingMax = currentSelection.get(Phase.MATCHING.val).size();
				screeningMax = currentSelection.get(Phase.SCREENING.val).size();
			}

			int matching = phaseToPredicatesMap.get(Phase.MATCHING.getVal()).size();

			//The current entry is better so continue
			if (matching < matchingMax) {
				continue;
			}

			//The this entry is better than current so update and continue
			if (matching > matchingMax) {
				tableNameToSelectedIndexMap.put(tableName, indexName);
				continue;
			}

			//Both the entries are equal. So go to screening
			int screening = phaseToPredicatesMap.get(Phase.SCREENING.val).size();

			if (screening <  screeningMax) {
				continue;
			}

			if (screening > screeningMax) {
				tableNameToSelectedIndexMap.put(tableName, indexName);
				continue;
			} 

			//Screening is also equal. So now go to Filter Factors.
			if (tableNameToSelectedIndexMap.containsKey(tableName)) {
				Map<String, List<Predicate>> currentSelection = indexToPhaseToPredicatesMap.get(tableName+"."+tableNameToSelectedIndexMap.get(tableName));
				double minFFCurrent = calculateLeastFilterFactor(currentSelection.get(Phase.MATCHING.val));
				double minFFThis = calculateLeastFilterFactor(phaseToPredicatesMap.get(Phase.MATCHING.val));

				//Min FF is better of the current value so continue
				if (minFFThis > minFFCurrent) {
					continue;
				} 

				//Min FF is better of this value so update and continue
				if (minFFThis < minFFCurrent) {
					tableNameToSelectedIndexMap.put(tableName, indexName);
					continue;
				}

				//Since none of the conditions satisfied minFF of both is equal. Go to minScreeningFF
				double minScreeningFFCurrent = calculateLeastFilterFactor(currentSelection.get(Phase.SCREENING.val));
				double minScreeningFFThis = calculateLeastFilterFactor(phaseToPredicatesMap.get(Phase.SCREENING.val));

				if (minScreeningFFThis > minScreeningFFCurrent) {
					continue;
				}

				if (minScreeningFFThis < minScreeningFFCurrent){
					tableNameToSelectedIndexMap.put(tableName, indexName);
					continue;
				}
				//Nothing satisfied at this point. Leave at this state

			} 

		}

		return tableNameToSelectedIndexMap;

	}

	
	private double calculateLeastFilterFactor(List<Predicate> predicates) {
		double minFF = Double.MAX_VALUE;
		for (Predicate predicate : predicates) {
			double filterFactor = predicate.getFf1();
			if (filterFactor < minFF) {
				minFF = filterFactor;
			}		
		}
		return minFF;
	}

	
	/**
	 * Performs In List Transformation
	 * @param allPredicates
	 * @param predicates
	 * @param tableToPredicatesMap
	 */
	private void inListTrasformation(List<Predicate> allPredicates, List<Predicate> predicates, Map<String, ArrayList<Predicate>> tableToPredicatesMap) {

		if (checkForInListTransformation) {

			String prevPredicate = getLhsOfPredicate(predicates.get(0).getText());
			for (Predicate predicate : predicates) {
				String lhs  = getLhsOfPredicate(predicate.getText());
				if (!prevPredicate.equalsIgnoreCase(lhs)) {
					evaluateIndex = false;
					/*		return;*/
				}
			}

			List<Predicate> inListTranformationPredicates = new ArrayList<Predicate>();

			for (int i = 0; i < predicates.size(); i++) {
				Predicate predicate = predicates.get(i);
				if (Predicate.Type.E.getVal() == predicate.getType() && predicate.isJoin() == false) {

					if (inListTranformationPredicates.contains(predicate)) {
						continue;
					}

					int j = i+1;
					if (j < predicates.size()) {

						String lhs = getLhsOfPredicate(predicate.getText());
						Predicate innerPredicate = predicates.get(j);
						String lhsInner = getLhsOfPredicate(innerPredicate.getText());

						while ( Predicate.Type.E.getVal() == innerPredicate.getType() && j < predicates.size() && predicate.isJoin() == false ) {
							if(lhs.equals(lhsInner)){
								if (inListTranformationPredicates.contains(predicate)) {
									inListTranformationPredicates.add(innerPredicate);
								} else {
									inListTranformationPredicates.add(predicate);
									inListTranformationPredicates.add(innerPredicate);
								}
							} 
							j++;
							if (j ==  predicates.size()) {
								continue;
							}
							innerPredicate = predicates.get(j);
							lhsInner = getLhsOfPredicate(innerPredicate.getText());

						}
					}
				}
			}

			for (int i = 0; i < predicates.size(); i++) {
				Predicate predicate = predicates.get(i);

				String lhs = getLhsOfPredicate(predicate.getText());

				if (inListTranformationPredicates.contains(predicate)) {
					String tableName = getTableNameFromPredicate(lhs);
					//	List<Predicate> inListPredicates = lhsToInListTransformationsMap.get(lhs);
					predicate.setInList(true);
					predicate.setType(Predicate.Type.I.getVal());
					String rhs = getRhsOfPredicate(predicate);
					StringBuilder inListTextBuilder = new StringBuilder();
					inListTextBuilder.append(predicate.getText()).append(" ");
					StringBuilder inListDescriptionBuilder = new StringBuilder();
					inListDescriptionBuilder.append(lhs).append(" ").append(IN).append(" ").append(OPENING_BRACE).append(" ").append(rhs);
					for (Predicate pred2 : inListTranformationPredicates) {
						if (pred2 != predicate) {
							String rhsInner = getRhsOfPredicate(pred2);
							inListTextBuilder.append(OR).append(" ").append(pred2.getText()).append(" ");
							inListDescriptionBuilder.append(" ").append(COMMA).append(" ").append(rhsInner);
							predicates.remove(pred2);
							allPredicates.remove(pred2);
							List<Predicate> tablePreds = tableToPredicatesMap.get(tableName);
							for (int j = 0; j < tablePreds.size(); j++) {
								Predicate tablePred = tablePreds.get(j);
								if (tablePred == pred2) {
									tableToPredicatesMap.get(tableName).remove(j);
								}
							}

						}
					}
					inListDescriptionBuilder.append(" ").append(CLOSING_BRACE);
					predicate.setText(inListTextBuilder.toString());
					predicate.setDescription(inListDescriptionBuilder.toString());
				}

			}

			for (Predicate predicate : predicates) {
				if (predicate.isInList() && predicate.getDescription() != null) {
					String val[] = predicate.getDescription().trim().split(" ");
					int noOfCommas = (val.length - 4)/2;
					double noOfElements = val.length - 4 - noOfCommas;
					double newFF = noOfElements / predicate.getCard1();
					predicate.setFf1(newFF);
				}

			}


			//Incase all the Or Predicates do not converge to a single In List. We will not use the Index
			if (predicates.size() > 1) {
				evaluateIndex = false;
			}
		}
	}

	/**
	 * Performs Reverse In List Transformation if applicable
	 * @param allPredicates
	 */
	private void reverseInListTransformation(List<Predicate> allPredicates) {
		for (Predicate predicate : allPredicates) {
			if (predicate.isInList()) {
				String predicateText[] = predicate.getText().trim().split("\\s+");
				if (predicateText.length == 5) {
					predicate.setType(Predicate.Type.E.getVal());
					predicate.setDescription(predicateText[0] + " = " + predicateText[3]);
					predicate.setInList(false);
				}
			}
		}
	}

	
	private String getLhsOfPredicate(String text) {
		String[] predicateText = text.trim().split("\\s+");
		return predicateText[0];
	}

	private String getRhsOfPredicate(Predicate predicate) {
		String[] predicateText = predicate.getText().trim().split("\\s+");
		//Length greater than 3 implies it is an reverse inlist transformation.
		if (predicateText.length > 3) {
			predicateText = predicate.getDescription().trim().split("\\s+");
		}
		return predicateText[2];
	}

	
	private Predicate processPredicate(String predicateText, Map<String, ArrayList<Predicate>> tableToPredicatesMap) throws DbmsError{
		if (predicateText == null) {
			throw new NullPointerException("Predicate Text Missing. Cannot process predicate");
		}
		Predicate predicate = new Predicate ();
		String val[] = predicateText.split("\\s");
		predicate.setText(predicateText);

		String lhs = val[0];

		if (!lhs.contains(".")){
			return processLiteralPredicate(predicateText);
		}

		Column column= getColumn(lhs);
		if (column == null) {
			throw new DbmsError("Invalid syntax for SELECT. Invalid predicate column " +  lhs);
		}
		predicate.setCard1(column.getColCard());
		predicate.setCol1Id(column.getColId());
		String lhsTable = getTableNameFromPredicate(lhs);

		String relOp = val[1];
		if (!relOp.equalsIgnoreCase(Predicate.RelationOperator.IN.getVal()) && val.length > 3) {
			throw new DbmsError("Invalid syntax for SELECT. Invalid predicate column In-List Values for predicate " +  predicateText);
		}

		double filterFactor;
		if (Predicate.RelationOperator.IN.getVal().equalsIgnoreCase(relOp)) {

			predicate.setType(Predicate.Type.I.getVal());

			predicate.setInList(true);
			List<Integer> inListInt = new ArrayList<Integer>();
			List<String> inListString = new ArrayList<String>();
			for (int i = 3; i < val.length -1; i++) {
				//Check value type for each column
				if (Column.ColType.INT == column.getColType()) {
					int inListVal ;
					try {
						inListVal = Integer.parseInt(val[i]);
					} catch (NumberFormatException e) {
						throw new DbmsError("Invalid syntax for SELECT. Invalid predicate column In-List Values for predicate " +  predicateText);
					}
					inListInt.add(inListVal);
				} else {
					inListString.add(val[i]);
				}

			}
			double noOfValues = val.length - 4;
			filterFactor = noOfValues / (double) column.getColCard();

		} else if (Predicate.RelationOperator.EQUAL.getVal().equals(relOp)) {
			predicate.setType(Predicate.Type.E.getVal());
			filterFactor =  calculateFF(column, relOp, null);

			String rhs = val[2]; 
			if (rhs.contains(".")) {
				Column rightColumn = getColumn(rhs);
				if (rightColumn == null) {
					throw new DbmsError("Invalid syntax for SELECT. Invalid predicate column " +  lhs);
				}

				if (rightColumn.getColType() != column.getColType()) {
					throw new DbmsError("Invalid syntax for SELECT. Invalid Join Column. Join column should be of the same type. '" +  lhs + "'.");
				}

				predicate.setJoin(true);
				double filterFactor2 = 1 / (double) rightColumn.getColCard();
				predicate.setCard2(rightColumn.getColCard());
				predicate.setFf2(filterFactor2);
				predicate.setCol2Id(rightColumn.getColId());

				String rhsTable = getTableNameFromPredicate(rhs);
				if (tableToPredicatesMap.containsKey(rhsTable)) {
					tableToPredicatesMap.get(lhsTable).add(predicate);
				}else {
					List<Predicate> predicates = new ArrayList<Predicate>();
					predicates.add(predicate);
					tableToPredicatesMap.put(rhsTable, (ArrayList<Predicate>) predicates);
				}

			}else {
				if (Column.ColType.INT.equals(column.getColType())) {
					try {
						int rhsDataVal = Integer.parseInt(rhs);
					} catch (NumberFormatException e) {
						throw new DbmsError("Invalid syntax for SELECT. Invalid predicate column Values for predicate " +  predicateText + ".");
					}
				} 
			}

		} else if (Predicate.RelationOperator.LESS_THAN.getVal().equals(relOp)) {
			predicate.setType(Predicate.Type.R.getVal());
			filterFactor = calculateFF(column, relOp, val[2]);

		} else if ( Predicate.RelationOperator.GREATER_THAN.getVal().equals(relOp)) {
			predicate.setType(Predicate.Type.R.getVal());
			filterFactor = calculateFF(column, relOp, val[2]);
		} else { 
			throw new DbmsError("Invalid syntax for SELECT. ");
		}

		predicate.setFf1(filterFactor);


		if (tableToPredicatesMap.containsKey(lhsTable)) {
			tableToPredicatesMap.get(lhsTable).add(predicate);
		} else {
			ArrayList<Predicate> tablePredicates = new ArrayList<Predicate>();
			tablePredicates.add(predicate);
			tableToPredicatesMap.put(lhsTable, tablePredicates);
		}

		return predicate;


	}

	private double calculateFF(Column column, String relOp , String value) throws DbmsError {
		double filterFactor = 0;
		if (Predicate.RelationOperator.EQUAL.getVal().equals(relOp)) {
			filterFactor = 1 / (double) column.getColCard();
		}

		if (Predicate.RelationOperator.GREATER_THAN.getVal().equals(relOp)) {
			double litValue, highKey,lowKey;
			if (Column.ColType.INT == column.getColType()) {
				litValue = Integer.parseInt(value);
				highKey = Integer.parseInt(column.getHiKey());
				lowKey = Integer.parseInt(column.getLoKey());
			} else {
				litValue = getBase64Value(value);
				highKey = getBase64Value(column.getHiKey());
				lowKey = getBase64Value(column.getLoKey());
			}
			filterFactor = (highKey - litValue) / (highKey - lowKey); 
			if (filterFactor > 1) {
				filterFactor = 1;
			}
			if (filterFactor < 0) {
				filterFactor = 0;
			}

		}

		if(Predicate.RelationOperator.LESS_THAN.getVal().equals(relOp)) {
			double litValue, highKey,lowKey;

			if (Column.ColType.INT == column.getColType()) {
				litValue = Integer.parseInt(value);
				highKey = Integer.parseInt(column.getHiKey());
				lowKey = Integer.parseInt(column.getLoKey());

			} else {
				litValue = getBase64Value(value);
				highKey = getBase64Value(column.getHiKey());
				lowKey = getBase64Value(column.getLoKey());
			}
			filterFactor = (litValue - lowKey) / (highKey - lowKey);

			if (filterFactor > 1) {
				filterFactor = 1;
			}
			if (filterFactor < 0) {
				filterFactor = 0;
			}
		}

		return filterFactor;
	}

	private Predicate processLiteralPredicate(String literalPredicate) {
		Predicate predicate = new Predicate();
		predicate.setCard1(0);
		predicate.setText(literalPredicate);

		String val[] = literalPredicate.trim().split("\\s+");
		String relOp = val[1];
		if (Predicate.RelationOperator.IN.getVal().equalsIgnoreCase(relOp)) {
			predicate.setInList(true);
			predicate.setType(Predicate.Type.I.getVal());
		} else if (Predicate.RelationOperator.EQUAL.getVal().equalsIgnoreCase(relOp)) {
			predicate.setType(Predicate.Type.E.getVal());
		} else {
			predicate.setType(Predicate.Type.R.getVal());
		}
		String lhs = val[0];
		String rhs = val[2];
		if (lhs.equalsIgnoreCase(rhs)) {
			predicate.setFf1(1);
		} else {
			predicate.setFf1(0);
		}
		predicate.setSequence(0);

		return predicate;
	}
	/**
	 * Constructs a matrix of Index vs Matching and Screening Predicates
	 * @param selectTableNames
	 * @param tableToPredicatesMap
	 * @return
	 * @throws DbmsError
	 */
	public Map<String, Map<String, List<Predicate>>> indexWisePhaseCalculation(List<String> selectTableNames, Map<String, ArrayList<Predicate>> tableToPredicatesMap) throws DbmsError{

		Map<String, Map<String, List<Predicate>>> tableNameToPredicatesMap = new HashMap<String, Map<String, List<Predicate>>>();
		for (String selectedTableName : selectTableNames) {
			Table table =  tableNameToTableMap.get(selectedTableName);


			for (Index idx	: table.getIndexes()) {
				String phase = Phase.MATCHING.getVal();
				Map<String, List<Predicate>> predicateTypeToPredicatesMap = new HashMap<String, List<Predicate>>();
				List<Predicate> matchingPredicates = new ArrayList<Predicate>();
				List<Predicate> screeningPredicates = new ArrayList<Predicate>();

				for (Index.IndexKeyDef indexKeyDef : idx.getIdxKey()) {
					Column column = table.getColumnFromId(indexKeyDef.colId);
					//TODO Decide if we need a per table list of preds or not. Else change logic
					List<Predicate> preds = tableToPredicatesMap.get(table.getTableName());
					if (preds ==  null || preds.isEmpty() == true) {
						phase = Phase.SCREENING.getVal();
						continue;
					}

					boolean flag = false;
					for (Predicate predicate : preds) {
						String[] text = predicate.getText().split("\\s+");
						//String lhs = text[0];
						//Column predicateLhsCol = getColumn(lhs);

						int predicateColId = predicate.getCol1Id();
						if (predicate.isJoin()) {
							String lhs = text[0];
							String lhsTableName = getTableNameFromPredicate(lhs);
							if (selectedTableName.equalsIgnoreCase(lhsTableName)) {
								predicateColId = predicate.getCol1Id();
							} else  {
								predicateColId = predicate.getCol2Id();
							}
						}

						if (predicateColId == column.getColId()) {
							if (phase.equalsIgnoreCase(Phase.MATCHING.getVal())) {
								String relOp = text[1];
								if(Predicate.RelationOperator.EQUAL.getVal().equalsIgnoreCase(relOp)){
									matchingPredicates.add(predicate);
								} else if(Predicate.RelationOperator.LESS_THAN.getVal().equalsIgnoreCase(relOp) || Predicate.RelationOperator.GREATER_THAN.getVal().equalsIgnoreCase(relOp)) {
									matchingPredicates.add(predicate);
									phase = Phase.SCREENING.getVal();
								} else if (Predicate.RelationOperator.IN.getVal().equalsIgnoreCase(relOp)) {
									matchingPredicates.add(predicate);
								}
							} else {
								//Phase is screening
								screeningPredicates.add(predicate);
							}
							flag = true;
						} 
					}
					//If no predicates matched the col then change the phase to screening
					if(flag == false) {
						phase = Phase.SCREENING.getVal();
					}
				}

				predicateTypeToPredicatesMap.put(Phase.MATCHING.getVal(),matchingPredicates);
				predicateTypeToPredicatesMap.put(Phase.SCREENING.getVal(),screeningPredicates);
				tableNameToPredicatesMap.put(table.getTableName()+"."+idx.getIdxName(), predicateTypeToPredicatesMap);


			}

		}

		for (Entry<String, Map<String, List<Predicate>>> entry : tableNameToPredicatesMap.entrySet()) {
			System.out.println(entry.getKey());
			for (Entry<String, List<Predicate>> entry2 : entry.getValue().entrySet()) {
				List<Predicate> predicate = (List<Predicate>) entry2.getValue();
				System.out.println("\t" + entry2.getKey());
				for (Predicate predicate2 : predicate) {
					System.out.println("\t\t" + predicate2.getText());
				}
			}
		}

		return tableNameToPredicatesMap;

	}


	public enum Phase {
		MATCHING("Matching") , SCREENING("Screening");
		private String val;

		Phase(String val) {
			this.val = val;
		}

		public String getVal() {
			return this.val;

		}
	}


	private int getBase64Value(String value) throws DbmsError {
		if (value == null) {
			throw new DbmsError("Cannot convert to Base 64");
		}
		char val[] = value.toUpperCase().toCharArray();
		int i = val[0] - 'A' + 1;
		if (val.length > 1) {
			int j = val[1] - 'A' + 1;
			return ( 26 * i ) + j;
		}
		return i;
	}

	@Nullable
	private Column getColumn(String selectColumn) throws DbmsError {

		String[] val = selectColumn.split("\\.");
		String tableName = val[0];
		if (!isTable(tableName)) { 
			throw new DbmsError("Table does not exist.");
		}
		String columnName = val[1];
		List<Column> columns = tableNameToTableMap.get(tableName).getColumns();
		for (Column col : columns) {
			if (col.getColName().equalsIgnoreCase(columnName)) {
				return col;
			}
		}
		return null;
	}	

	@Nullable
	private Table getTable(String tableName) {
		if (tableNameToTableMap.containsKey(tableName)) {
			if (tableNameToTableMap.get(tableName).delete == false) {
				return tableNameToTableMap.get(tableName);
			}
		}
		return null;
	}

	private String getTableNameFromPredicate(String col) {
		String[] val = col.split("\\.");
		String tableName = val[0];
		return tableName;
	}



	private boolean isColumn(String columnName, Table table) {
		for (Column col : table.getColumns()) {
			if (col.getColName().equalsIgnoreCase(columnName)) {
				return true;
			}
		}
		return false;
	}

	private boolean isTable(String tableName) {
		if (tableNameToTableMap.containsKey(tableName)) {
			if (tableNameToTableMap.get(tableName).delete == true) {
				return false;
			}
			return true;
		}
		return false;
	}

	private String runstats(String sql, StringTokenizer tokenizer) throws DbmsError {
		if (!tokenizer.hasMoreTokens()) {
			throw new DbmsError("Invalid syntax for RUNSTATS. Reason: Table name missing. '" + sql + "'.");
		}
		String tableName = tokenizer.nextToken().toUpperCase();
		Table table = null;
		for (Table tab : tables) {
			if (tab.getTableName().equalsIgnoreCase(tableName) && tab.delete == false) {
				table = tab;
				break;
			}
		}

		//If Table is null implies no such table exists
		if(table == null) {
			throw new DbmsError("Cannot RUNSTATS. Reason: Table '" + tableName
					+ "' does not exist. '" + sql + "'.");		
		}

		if (!tokenizer.hasMoreTokens() || !SEMI_COLON.equals(tokenizer.nextToken())) {
			throw new DbmsError("Invalid syntax for RUNSTATS. Reason: ; missing. '" + sql + "'.");
		}

		calculateRunstats(tableName);
		out.println("RUNSTATS for table " + tableName + " collected successfully");
		return tableName;
	}

	private void calculateRunstats(String tableName){
		Table table = tableNameToTableMap.get(tableName);
		ArrayList<String> tableData = table.getData();
		int tableCard = tableData.size();
		table.setTableCard(tableCard);

		Map<Integer, SortedSet<String>> colIdToValuesMap = new HashMap<Integer, SortedSet<String>>();
		for (String row : tableData) {
			String[] rowData = row.trim().split("\\s+");
			for (Column col : table.getColumns()) {
				int colId = col.getColId();
				String data = rowData[colId];
				if (data.equals("-")) {
					continue;
				}


				if (Column.ColType.INT == col.getColType()) {
					data = String.format("%010d", Integer.parseInt(data));
				}


				if (colIdToValuesMap.containsKey(colId)) {
					colIdToValuesMap.get(colId).add(data);
				}else {
					SortedSet<String> value = new TreeSet<String>();
					value.add(data);
					colIdToValuesMap.put(colId, value);
				}
				SortedSet<String> colValues = colIdToValuesMap.get(colId);

				col.setColCard(colValues.size());

				if (Column.ColType.INT == col.getColType()) {
					int last = Integer.parseInt(colValues.last());
					col.setHiKey(String.valueOf(last));
					int first = Integer.parseInt(colValues.first());
					col.setLoKey(String.valueOf(first));
				}else {
					col.setHiKey(colValues.last());
					col.setLoKey(colValues.first());
				}
			}
		}

	}

	private void createIndex(String sql, StringTokenizer tokenizer,
			Boolean isUnique) throws DbmsError {

		String ON = "ON";
		String indexName = tokenizer.nextToken().toUpperCase();
		if (!Character.isAlphabetic(indexName.charAt(0))) {
			throw new DbmsError("Invalid index identifier " + indexName + ". '"	+ sql + "'.");
		}

		// Next Check if ON keyword exists or not
		if (tokenizer.hasMoreTokens() == false
				|| ON.equalsIgnoreCase(tokenizer.nextToken()) == false) {
			throw new DbmsError("Invalid syntax for " + indexName
					+ ". Reason: ON Keyword missing. '" + sql + "'.");
		}

		String tableName = "";
		Table table = null;
		if (!tokenizer.hasMoreTokens()) {
			throw new DbmsError("Cannot create Index '" + indexName
					+ "'. Reason: TableName not specified. '" + sql + "'.");
		}

		tableName = tokenizer.nextToken().toUpperCase();


		for (Table tab : tables) {
			if (tab.getTableName().equals(tableName) && !tab.delete) {
				table = tab;
				break;
			}
			for (Index idx : tab.getIndexes()) {
				if (idx.getIdxName().equals(indexName) && !idx.delete) {
					throw new DbmsError(
							"Cannot create Index '"	+ indexName
							+ "'. Reason: Index with the same name already exists. '" + sql + "'.");
				}
			}

		}


		// If Table is null implies no such table exists
		if (table == null) {
			throw new DbmsError("Cannot create Index '" + indexName
					+ "'. Reason: Table '" + tableName + "' does not exist. '"	+ sql + "'.");
		}


	

		// Check for opening bracket
		if (!tokenizer.hasMoreTokens() || !tokenizer.nextToken().equalsIgnoreCase("(")) {
			throw new DbmsError("Cannot create Index '" + indexName + "'. Reason: Invalid Syntax. '(' missing. '" + sql);
		}

		// Check if columns exist or not
		if (!tokenizer.hasMoreTokens()) {
			throw new DbmsError("Cannot create Index '" + indexName	+ "'. Reason: Invalid Syntax. '" + sql);
		}

		Index index = new Index(indexName);
		index.setIsUnique(isUnique);

		List<Index.IndexKeyDef> indexKeyDefs = new ArrayList<Index.IndexKeyDef>();
		ArrayList<Column> tableColumns = table.getColumns();

		boolean done = false;
		int colId = 0;
		while (!done) {
			String columnName = tokenizer.nextToken();
			colId++;

			Column column = null;
			for (Column col : tableColumns) {
				if (col.getColName().equalsIgnoreCase(columnName)) {
					column = col;
				}
			}
			if (column == null) {
				throw new DbmsError("Cannot create Index '" + indexName	+ "'. Reason: Index column " + columnName
						+ " not identified as a column in table. '" + sql + "'.");
			}
			List<Index.IndexKeyDef> existingIndexKeyDefs = index.getIdxKey();
			for (Index.IndexKeyDef indexKeyDef : existingIndexKeyDefs) {
				if (indexKeyDef.colId == column.getColId()) {
					throw new DbmsError("Cannot create Index '" + indexName	+ "'. Reason: Index column " + columnName + " repeated. '" + sql + "'.");
				}
			}
			Index.IndexKeyDef indexKeyDef = index.new IndexKeyDef();
			indexKeyDef.idxColPos = colId;
			indexKeyDef.colId = column.getColId();

			if (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (DESC.equalsIgnoreCase(token)) {
					indexKeyDef.descOrder = true;
					if (tokenizer.hasMoreElements()) {
						token = tokenizer.nextToken();
					} else {
						throw new DbmsError("Cannot create Index '" + indexName + "'. Invalid syntax. '" + sql + "'.");
					}
				}
				index.addIdxKey(indexKeyDef);

				if (token.equalsIgnoreCase(",")) {
					continue;
				} else if (token.equalsIgnoreCase(")")) {
					done = true;
				} else {
					throw new DbmsError("Cannot create Index '" + indexName	+ "'. Invalid syntax. '" + sql + "'.");
				}
			}

		}

		// Check for ;
		if (!tokenizer.hasMoreTokens()	&& !SEMI_COLON.equalsIgnoreCase(tokenizer.nextToken()))  {
			throw new DbmsError("Cannot create Index '" + indexName	+ "'. Reason: ';' missing. '" + sql + "'.");
		}

		// Check if number of columns in the index are not greater than Number
		// of cols in the table
		if (indexKeyDefs.size() > table.getNumColumns()) {
			throw new DbmsError( "Cannot create Index '" + indexName + "'. Reason: Index cannot have more columns than the table. '" + sql + "'.");
		}

		populateIndex(table, index);
		table.addIndex(index);
		out.println("Index " + indexName + " created successfully on Table " + tableName);
	}

	/**
	 * Populate IndexKey Value for the given Index
	 * @param table
	 * @param index
	 * @throws DbmsError
	 */
	private void populateIndex(Table table, Index index) throws DbmsError {
		List<String> rows = table.getData();
		for (String row : rows) {
			String[] rowChars = row.trim().split("\\s+");
			String rid = rowChars[0];
			String compositeKey = createCompositeKey(index, table.getColumns(),	row);

			if (index.getIsUnique()) {
				try {
					checkForUnique(index, compositeKey);
				} catch (Exception e) {
					throw new DbmsError("Cannot create Index '"
							+ index.getIdxName() + "'. Reason: "
							+ e.getMessage());
				}

			}
			Index.IndexKeyVal indexkeyVal = index.new IndexKeyVal();
			indexkeyVal.rid = Integer.parseInt(rid);
			indexkeyVal.value = compositeKey;
			index.addKey(indexkeyVal);
		}
		sortIndex(index);

	}

	/**
	 * For the given row and Index this function will construct a composite key 
	 * @param index
	 * @param columns
	 * @param row
	 * @return
	 * @throws DbmsError 
	 */
	private String createCompositeKey(Index index, List<Column> columns, String row) throws DbmsError {

		String[] rowTokens = row.trim().split("\\s+");
		StringBuilder indexValueBuilder = new StringBuilder();
		ArrayList<Index.IndexKeyDef> indexKeyDefs = index.getIdxKey();
		for (Index.IndexKeyDef indexDef : indexKeyDefs ) {
			String value = rowTokens[indexDef.colId];
			Column column = columns.get(indexDef.colId - 1);

			if (NULL.equals(value)) {

				if (Column.ColType.INT == column.getColType()) {
					indexValueBuilder.append("~").append(String.format("%9s", ""));
				}
				if (Column.ColType.CHAR == column.getColType()) {
					int length = column.getColLength() - 1; 
					indexValueBuilder.append("~").append(String.format("%" + length + "s", ""));
				}

				continue;
			}

			if (indexDef.descOrder) {
				if (Column.ColType.CHAR == column.getColType()) {
					char[] chars = value.toCharArray();
					char[] descChar = new char[column.getColLength()];
					int i = 0;
					for (char c : chars) {
						if (c >= 'a' && c <= 'z') {
							int d = 'z' - c + 'a';
							descChar[i] = (char) d;
							i++;
						}
						if (c >= 'A' && c <= 'Z') {
							int d = 'Z' - c + 'A';
							descChar[i] = (char) d;
							i++;
						}
					}
					indexValueBuilder.append(new String(descChar));
				}
				if (Column.ColType.INT == column.getColType()) {
					long desc = 9999999999L - Integer.parseInt(value);
					indexValueBuilder.append(String.valueOf(desc));
				}
			} else {

				if (Column.ColType.INT == column.getColType()) {
					indexValueBuilder.append(String.format("%010d",
							Integer.parseInt(value)));
				}
				if (Column.ColType.CHAR == column.getColType()) {
					indexValueBuilder.append(Arrays.copyOf(value.toCharArray(),
							column.getColLength()));
				}

			}
		}
		return indexValueBuilder.toString();
	}


	/**
	 * For the given Value will check if Index with the same value exists or not.
	 * Will throw an error incase duplicate values are found.
	 * @param index
	 * @param value
	 * @throws DbmsError
	 */
	private void checkForUnique(Index index, String value) throws DbmsError {

		if(index.getIdxKey().size() == 1 && "~".equals(value.trim())) {
			throw new DbmsError("Unique Constraint on index "
					+ index.getIdxName() + " violated. Cannot have null in a single column index.");
		}

		for (Index.IndexKeyVal indexValues : index.getKeys()) {
			if (indexValues.value.equals(value)) {
				throw new DbmsError("Unique Constraint on index "
						+ index.getIdxName() + " violated.");
			}
		}
	}

	/**
	 * Sorts the given index in ascending order
	 * @param index
	 */
	private void sortIndex(Index index) {
		Collections.sort(index.getKeys(), new Comparator<Index.IndexKeyVal>() {

			@Override
			public int compare(Index.IndexKeyVal o1, Index.IndexKeyVal o2) {
				return o1.value.compareToIgnoreCase(o2.value);
			}
		});

	}



	/**
	 * Loads tables to memory
	 * 
	 * @throws Exception
	 */
	private void loadTables() throws Exception {
		// Get all the available tables in the "tables" directory
		File tableDir = new File(TABLE_FOLDER_NAME);
		if (tableDir.exists() && tableDir.isDirectory()) {
			for (File tableFile : tableDir.listFiles()) {
				// For each file check if the file extension is ".tab"
				String tableName = tableFile.getName();
				int periodLoc = tableName.lastIndexOf(".");
				String tableFileExt = tableName.substring(tableName
						.lastIndexOf(".") + 1);
				if (tableFileExt.equalsIgnoreCase("tab")) {
					// If it is a ".tab" file, create a table structure
					Table table = new Table(tableName.substring(0, periodLoc));
					Scanner in = new Scanner(tableFile);

					try {
						// Read the file to get Column definitions
						int numCols = Integer.parseInt(in.nextLine());

						for (int i = 0; i < numCols; i++) {
							StringTokenizer tokenizer = new StringTokenizer(
									in.nextLine());
							String name = tokenizer.nextToken();
							String type = tokenizer.nextToken();
							boolean nullable = Boolean.parseBoolean(tokenizer
									.nextToken());
							switch (type.charAt(0)) {
							case 'C':
								table.addColumn(new Column(i + 1, name,
										Column.ColType.CHAR, Integer
										.parseInt(type.substring(1)),
										nullable));
								break;
							case 'I':
								table.addColumn(new Column(i + 1, name,
										Column.ColType.INT, 4, nullable));
								break;
							default:
								break;
							}
						}

						// Read the file for index definitions
						int numIdx = Integer.parseInt(in.nextLine());
						for (int i = 0; i < numIdx; i++) {
							StringTokenizer tokenizer = new StringTokenizer(
									in.nextLine());
							Index index = new Index(tokenizer.nextToken());
							index.setIsUnique(Boolean.parseBoolean(tokenizer
									.nextToken()));

							int idxColPos = 1;
							while (tokenizer.hasMoreTokens()) {
								String colDef = tokenizer.nextToken();
								Index.IndexKeyDef def = index.new IndexKeyDef();
								def.idxColPos = idxColPos;
								def.colId = Integer.parseInt(colDef.substring(
										0, colDef.length() - 1));
								switch (colDef.charAt(colDef.length() - 1)) {
								case 'A':
									def.descOrder = false;
									break;
								case 'D':
									def.descOrder = true;
									break;
								default:
									break;
								}

								index.addIdxKey(def);
								idxColPos++;
							}

							table.addIndex(index);
							loadIndex(table, index);
						}

						// Read the data from the file
						int numRows = Integer.parseInt(in.nextLine());
						for (int i = 0; i < numRows; i++) {
							table.addData(in.nextLine());
						}

						// Read RUNSTATS from the file
						while(in.hasNextLine()) {
							String line = in.nextLine();
							StringTokenizer toks = new StringTokenizer(line);
							if(toks.nextToken().equals("STATS")) {
								String stats = toks.nextToken();
								if(stats.equals("TABCARD")) {
									table.setTableCard(Integer.parseInt(toks.nextToken()));
								} else if (stats.equals("COLCARD")) {
									Column col = table.getColumns().get(Integer.parseInt(toks.nextToken()));
									col.setColCard(Integer.parseInt(toks.nextToken()));
									col.setHiKey(toks.nextToken());
									col.setLoKey(toks.nextToken());
								} else {
									throw new DbmsError("Invalid STATS.");
								}
							} else {
								throw new DbmsError("Invalid STATS.");
							}
						}
					} catch (DbmsError ex) {
						throw ex;
					} catch (Exception ex) {
						throw new DbmsError("Invalid table file format.");
					} finally {
						in.close();
					}
					tables.add(table);
					tableNameToTableMap.put(table.getTableName(), table);
					/*
					IndexList il = new IndexList();
					il.printIndex(table, out);*/
				}
			}
		} else {
			throw new FileNotFoundException(
					"The system cannot find the tables directory specified.");
		}
	}

	/**
	 * Loads specified table to memory
	 * 
	 * @throws DbmsError
	 */
	private void loadIndex(Table table, Index index) throws DbmsError {
		try {
			Scanner in = new Scanner(new File(TABLE_FOLDER_NAME,
					table.getTableName() + index.getIdxName() + INDEX_FILE_EXT));
			String def = in.nextLine();
			String rows = in.nextLine();

			while (in.hasNext()) {
				String line = in.nextLine();
				Index.IndexKeyVal val = index.new IndexKeyVal();
				val.rid = Integer.parseInt(new StringTokenizer(line)
				.nextToken());
				val.value = line.substring(line.indexOf("'") + 1,
						line.lastIndexOf("'"));
				index.addKey(val);
			}
			in.close();

		} catch (Exception ex) {
			throw new DbmsError("Invalid index file format.");
		}
	}

	/**
	 * CREATE TABLE
	 * <table name>
	 * ( <col name> < CHAR ( length ) | INT > <NOT NULL> ) ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void createTable(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			// Check the table name
			String tok = tokenizer.nextToken().toUpperCase();
			if (Character.isAlphabetic(tok.charAt(0))) {
				// Check if the table already exists
				for (Table tab : tables) {
					if (tab.getTableName().equals(tok) && !tab.delete) {
						throw new DbmsError("Table " + tok
								+ "already exists. '" + sql + "'.");
					}
				}

				// Create a table instance to store data in memory
				Table table = new Table(tok.toUpperCase());

				// Check for '('
				tok = tokenizer.nextToken();
				if (tok.equals("(")) {
					// Look through the column definitions and add them to the
					// table in memory
					boolean done = false;
					int colId = 1;
					while (!done) {
						tok = tokenizer.nextToken();
						if (Character.isAlphabetic(tok.charAt(0))) {
							String colName = tok;
							Column.ColType colType = Column.ColType.INT;
							int colLength = 4;
							boolean nullable = true;

							tok = tokenizer.nextToken();
							if (tok.equalsIgnoreCase("INT")) {
								// use the default Column.ColType and colLength

								// Look for NOT NULL or ',' or ')'
								tok = tokenizer.nextToken();
								if (tok.equalsIgnoreCase("NOT")) {
									// look for NULL after NOT
									tok = tokenizer.nextToken();
									if (tok.equalsIgnoreCase("NULL")) {
										nullable = false;
									} else {
										throw new NoSuchElementException();
									}

									tok = tokenizer.nextToken();
									if (tok.equals(",")) {
										// Continue to the next column
									} else if (tok.equalsIgnoreCase(")")) {
										done = true;
									} else {
										throw new NoSuchElementException();
									}
								} else if (tok.equalsIgnoreCase(",")) {
									// Continue to the next column
								} else if (tok.equalsIgnoreCase(")")) {
									done = true;
								} else {
									throw new NoSuchElementException();
								}
							} else if (tok.equalsIgnoreCase("CHAR")) {
								colType = Column.ColType.CHAR;

								// Look for column length
								tok = tokenizer.nextToken();
								if (tok.equals("(")) {
									tok = tokenizer.nextToken();
									try {
										colLength = Integer.parseInt(tok);
									} catch (NumberFormatException ex) {
										throw new DbmsError(
												"Invalid table column length for "
														+ colName + ". '" + sql
														+ "'.");
									}

									// Check for the closing ')'
									tok = tokenizer.nextToken();
									if (!tok.equals(")")) {
										throw new DbmsError(
												"Invalid table column definition for "
														+ colName + ". '" + sql
														+ "'.");
									}

									// Look for NOT NULL or ',' or ')'
									tok = tokenizer.nextToken();
									if (tok.equalsIgnoreCase("NOT")) {
										// Look for NULL after NOT
										tok = tokenizer.nextToken();
										if (tok.equalsIgnoreCase("NULL")) {
											nullable = false;

											tok = tokenizer.nextToken();
											if (tok.equals(",")) {
												// Continue to the next column
											} else if (tok
													.equalsIgnoreCase(")")) {
												done = true;
											} else {
												throw new NoSuchElementException();
											}
										} else {
											throw new NoSuchElementException();
										}
									} else if (tok.equalsIgnoreCase(",")) {
										// Continue to the next column
									} else if (tok.equalsIgnoreCase(")")) {
										done = true;
									} else {
										throw new NoSuchElementException();
									}
								} else {
									throw new DbmsError(
											"Invalid table column definition for "
													+ colName + ". '" + sql
													+ "'.");
								}
							} else {
								throw new NoSuchElementException();
							}

							// Everything is ok. Add the column to the table
							table.addColumn(new Column(colId, colName, colType,
									colLength, nullable));
							colId++;
						} else {
							// if(colId == 1) {
							throw new DbmsError(
									"Invalid table column identifier " + tok
									+ ". '" + sql + "'.");
							// }
						}
					}

					// Check for the semicolon
					tok = tokenizer.nextToken();
					if (!tok.equals(";")) {
						throw new NoSuchElementException();
					}

					// Check if there are more tokens
					if (tokenizer.hasMoreTokens()) {
						throw new NoSuchElementException();
					}

					if (table.getNumColumns() == 0) {
						throw new DbmsError(
								"No column descriptions specified. '" + sql
								+ "'.");
					}

					// The table is stored into memory when this program exists.
					tables.add(table);
					tableNameToTableMap.put(table.getTableName(), table);
					out.println("Table " + table.getTableName()
							+ " was created.");
				} else {
					throw new NoSuchElementException();
				}
			} else {
				throw new DbmsError("Invalid table identifier " + tok + ". '"
						+ sql + "'.");
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid CREATE TABLE statement. '" + sql
					+ "'.");
		}
	}

	/**
	 * INSERT INTO
	 * <table name>
	 * VALUES ( val1 , val2, .... ) ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void insertInto(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			String tok = tokenizer.nextToken();
			if (tok.equalsIgnoreCase("INTO")) {
				tok = tokenizer.nextToken().trim().toUpperCase();
				Table table = null;
				for (Table tab : tables) {
					if (tab.getTableName().equals(tok)) {
						table = tab;
						break;
					}
				}

				if (table == null) {
					throw new DbmsError("Table " + tok + " does not exist.");
				}

				tok = tokenizer.nextToken();
				if (tok.equalsIgnoreCase("VALUES")) {
					tok = tokenizer.nextToken();
					if (tok.equalsIgnoreCase("(")) {
						tok = tokenizer.nextToken();
						String values = String.format("%3s", table.getData()
								.size() + 1)
								+ " ";
						int colId = 0;
						boolean done = false;
						while (!done) {
							if (tok.equals(")")) {
								done = true;
								break;
							} else if (tok.equals(",")) {
								// Continue to the next value
							} else {
								if (colId == table.getNumColumns()) {
									throw new DbmsError(
											"Invalid number of values were given.");
								}

								Column col = table.getColumns().get(colId);

								if (tok.equals("-") && !col.isColNullable()) {
									throw new DbmsError(
											"A NOT NULL column cannot have null. '"
													+ sql + "'.");
								}

								if (col.getColType() == Column.ColType.INT) {
									try {
										if(!tok.equals("-")) {
											int temp = Integer.parseInt(tok);
										}
									} catch (Exception ex) {
										throw new DbmsError(
												"An INT column cannot hold a CHAR. '"
														+ sql + "'.");
									}

									tok = String.format("%10s", tok.trim());
								} else if (col.getColType() == Column.ColType.CHAR) {
									int length = tok.length();
									if (length > col.getColLength()) {
										throw new DbmsError(
												"A CHAR column cannot exceede its length. '"
														+ sql + "'.");
									}

									tok = String.format(
											"%-" + col.getColLength() + "s",
											tok.trim());
								}

								values += tok + " ";
								colId++;
							}
							tok = tokenizer.nextToken().trim();
						}

						if (colId != table.getNumColumns()) {
							throw new DbmsError(
									"Invalid number of values were given.");
						}

						// Check for the semicolon
						tok = tokenizer.nextToken();
						if (!tok.equals(";")) {
							throw new NoSuchElementException();
						}

						// Check if there are more tokens
						if (tokenizer.hasMoreTokens()) {
							throw new NoSuchElementException();
						}

						// insert the value to table

						List<Index> indexes = table.getIndexes();
						for (Index index : indexes) {
							String compositeKey = createCompositeKey(index, table.getColumns(), values);
							if (index.getIsUnique()) {
								try {
									checkForUnique(index, compositeKey);
								} catch (DbmsError e) {
									throw new DbmsError("Cannot insert the given data in the row. Reason: " + e.getMessage() + " '" + sql + "'.");
								}
							}
							String rid = values.trim().split("\\s+")[0];
							Index.IndexKeyVal indexKey = index.new IndexKeyVal();
							indexKey.rid = Integer.parseInt(rid);
							indexKey.value = compositeKey;
							index.addKey(indexKey);
							sortIndex(index);
						}

						table.addData(values);
						out.println("One line was saved to the table. "
								+ table.getTableName() + ": " + values);
					} else {
						throw new NoSuchElementException();
					}
				} else {
					throw new NoSuchElementException();
				}
			} else {
				throw new NoSuchElementException();
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid INSERT INTO statement. '" + sql + "'.");
		}
	}

	/**
	 * DROP TABLE
	 * <table name>
	 * ;
	 * 
	 * @param sql
	 * @param tokenizer
	 * @throws Exception
	 */
	private void dropTable(String sql, StringTokenizer tokenizer)
			throws Exception {
		try {
			// Get table name
			String tableName = tokenizer.nextToken();

			// Check for the semicolon
			String tok = tokenizer.nextToken();
			if (!tok.equals(";")) {
				throw new NoSuchElementException();
			}

			// Check if there are more tokens
			if (tokenizer.hasMoreTokens()) {
				throw new NoSuchElementException();
			}

			// Delete the table if everything is ok
			boolean dropped = false;
			for (Table table : tables) {
				if (table.getTableName().equalsIgnoreCase(tableName) && !table.delete) {
					table.delete = true;
					dropped = true;
					for (Index index : table.getIndexes()) {
						index.delete = true;
						int numIndexes = table.getNumIndexes();
						table.setNumIndexes(--numIndexes);
					}

					break;
				}
			}

			if (dropped) {
				out.println("Table " + tableName + " was dropped.");
			} else {
				throw new DbmsError("Table " + tableName + "does not exist. '" + sql + "'."); 
			}
		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid DROP TABLE statement. '" + sql + "'.");
		}

	}

	public void dropIndex(String sql, StringTokenizer tokenizer)
			throws Exception {

		try {
			// Get table name
			String indexName = tokenizer.nextToken();

			// Check for the semicolon
			if (!tokenizer.hasMoreTokens() || !SEMI_COLON.equals(tokenizer.nextToken())) {
				throw new DbmsError("Invalid syntax for DROP. Reason: ; missing. '" + sql.toUpperCase() + "'.");
			}

			// Check if there are more tokens
			if (tokenizer.hasMoreTokens()) {
				throw new NoSuchElementException();
			}

			// Delete the table if everything is ok
			Index index = null;
			for (Table table : tables) {

				for (Index idx : table.getIndexes()) {
					if(idx.getIdxName().equalsIgnoreCase(indexName) && idx.delete == false) {
						index = idx;
						int numIndexes = table.getNumIndexes();
						index.delete = true;
						table.setNumIndexes(--numIndexes);
						break;
					}
				}
			}

			if (index ==  null) {
				throw new DbmsError("Index " + indexName + "does not exist. '" + sql + "'.");
			}
			out.println("Index " + indexName + "deleted successfully");

		} catch (NoSuchElementException ex) {
			throw new DbmsError("Invalid DROP INDEX statement. '" + sql + "'.");
		}


	}

	private void printRunstats(String tableName) {
		for (Table table : tables) {
			if (table.getTableName().equals(tableName)) {
				out.println("TABLE CARDINALITY: " + table.getTableCard());
				for (Column column : table.getColumns()) {
					out.println(column.getColName());
					out.println("\tCOLUMN CARDINALITY: " + column.getColCard());
					out.println("\tCOLUMN HIGH KEY: " + column.getHiKey());
					out.println("\tCOLUMN LOW KEY: " + column.getLoKey());
				}
				break;
			}
		}
	}

	private void storeTableFile(Table table) throws FileNotFoundException {
		File tableFile = new File(TABLE_FOLDER_NAME, table.getTableName()
				+ TABLE_FILE_EXT);

		// Delete the file if it was marked for deletion
		if (table.delete) {
			try {
				tableFile.delete();
			} catch (Exception ex) {
				out.println("Unable to delete table file for "
						+ table.getTableName() + ".");
			}

			// Delete the index files too
			for (Index index : table.getIndexes()) {
				File indexFile = new File(TABLE_FOLDER_NAME, table.getTableName()
						+ index.getIdxName() + INDEX_FILE_EXT);

				try {
					indexFile.delete();
				} catch (Exception ex) {
					out.println("Unable to delete table file for "
							+ indexFile.getName() + ".");
				}
			}
		} else {
			// Create the table file writer
			PrintWriter out = new PrintWriter(tableFile);

			// Write the column descriptors
			out.println(table.getNumColumns());
			for (Column col : table.getColumns()) {
				if (col.getColType() == Column.ColType.INT) {
					out.println(col.getColName() + " I " + col.isColNullable());
				} else if (col.getColType() == Column.ColType.CHAR) {
					out.println(col.getColName() + " C" + col.getColLength()
							+ " " + col.isColNullable());
				}
			}

			// Write the index info
			out.println(table.getNumIndexes());
			for (Index index : table.getIndexes()) {
				if(!index.delete) {
					String idxInfo = index.getIdxName() + " " + index.getIsUnique()
							+ " ";

					for (Index.IndexKeyDef def : index.getIdxKey()) {
						idxInfo += def.colId;
						if (def.descOrder) {
							idxInfo += "D ";
						} else {
							idxInfo += "A ";
						}
					}
					out.println(idxInfo);
				}
			}

			// Write the rows of data
			out.println(table.getData().size());
			for (String data : table.getData()) {
				out.println(data);
			}

			// Write RUNSTATS
			out.println("STATS TABCARD " + table.getTableCard());
			for (int i = 0; i < table.getColumns().size(); i++) {
				Column col = table.getColumns().get(i);
				if(col.getHiKey() == null)
					col.setHiKey("-");
				if(col.getLoKey() == null)
					col.setLoKey("-");
				out.println("STATS COLCARD " + i + " " + col.getColCard() + " " + col.getHiKey() + " " + col.getLoKey());
			}

			out.flush();
			out.close();
		}

		// Save indexes to file
		for (Index index : table.getIndexes()) {

			File indexFile = new File(TABLE_FOLDER_NAME, table.getTableName()
					+ index.getIdxName() + INDEX_FILE_EXT);

			// Delete the file if it was marked for deletion
			if (index.delete) {
				try {
					indexFile.delete();
				} catch (Exception ex) {
					out.println("Unable to delete index file for "
							+ indexFile.getName() + ".");
				}
			} else {
				PrintWriter out = new PrintWriter(indexFile);
				String idxInfo = index.getIdxName() + " " + index.getIsUnique()
						+ " ";

				// Write index definition
				for (Index.IndexKeyDef def : index.getIdxKey()) {
					idxInfo += def.colId;
					if (def.descOrder) {
						idxInfo += "D ";
					} else {
						idxInfo += "A ";
					}
				}
				out.println(idxInfo);

				// Write index keys
				out.println(index.getKeys().size());
				for (Index.IndexKeyVal key : index.getKeys()) {
					String rid = String.format("%3s", key.rid);
					out.println(rid + " '" + key.value + "'");
				}

				out.flush();
				out.close();

			}
		}
	}
}
