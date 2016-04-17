
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CS 267 - Project - Implements a Table for the DBMS.
 */
public class Table {
	private String tableName;
	private int numColumns;
	private int numIndexes;
	private int tableCard;
	private ArrayList<Column> columns;
	private ArrayList<Index> indexes;
	private ArrayList<String> data;
	private Map<Integer, Column> colIdToColumnMap;
	private Map<String, List<Column>> indexNameToColumnMap;
	private Map<String, Index> indexNameToIndexMap;
	
	public boolean delete = false;

	public Table(String tableName) {
		this.tableName = tableName;
		numColumns = 0;
		numIndexes = 0;
		tableCard = 0;
		columns = new ArrayList<Column>();
		indexes = new ArrayList<Index>();
		data = new ArrayList<String>();
		colIdToColumnMap = new HashMap<Integer, Column>();
		indexNameToColumnMap = new HashMap<String, List<Column>>();
		indexNameToIndexMap = new HashMap<String, Index>();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getNumColumns() {
		return numColumns;
	}

	public void setNumColumns(int numColumns) {
		this.numColumns = numColumns;
	}

	public int getNumIndexes() {
		return numIndexes;
	}

	public void setNumIndexes(int numIndexes) {
		this.numIndexes = numIndexes;
	}

	public int getTableCard() {
		return tableCard;
	}

	public void setTableCard(int tableCard) {
		this.tableCard = tableCard;
	}

	public ArrayList<Column> getColumns() {
		return columns;
	}

	public void addColumn(Column column) {
		columns.add(column);
		colIdToColumnMap.put(column.getColId(), column);
		numColumns++;
	}

	public ArrayList<Index> getIndexes() {
		return indexes;
	}

	public void addIndex(Index index) {
		indexes.add(index);
		indexNameToIndexMap.put(index.getIdxName(), index);
		List<Column> columns = new ArrayList<>();
		for (Index.IndexKeyDef indexKeyDef : index.getIdxKey()) {
			Column col = colIdToColumnMap.get(indexKeyDef.colId);
			columns.add(col);
		}
		indexNameToColumnMap.put(index.getIdxName(), columns);
		numIndexes++;
	}

	public ArrayList<String> getData() {
		return data;
	}

	public void addData(String values) {
		data.add(values);
	}
	
	public Column getColumnFromId(int colId) {
		return colIdToColumnMap.get(colId);
	}
	
	public List<Column> getIndexedColumnsFromIndexName(String indexName) {
		return indexNameToColumnMap.get(indexName);
	}
	
	public Index getIndex(String indexName) {
		return indexNameToIndexMap.get(indexName);
	}
}
