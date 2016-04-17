import java.text.DecimalFormat;
import java.util.ArrayList;

public class Predicate {

	public boolean inList = false; // true if predicate is an inlist

	public boolean join; // true if join predicate, false if local predicate

	// Related to output table
	public char type = ' '; // E (equal), R (range), I (IN list)

	public int card1; // left column cardinality
	public int card2; // right column cardinality

	public double ff1; // left column filter factor
	public double ff2; // right column filter factor

	public int sequence = 999; // order of predicate evaluation

	public String text = ""; // original text
	public String description = ""; // description for added predicates and
	// other notes
	public RelationOperator relationOperator = RelationOperator.EQUAL;
	public Predicate predicate;
	private int col1Id;
	private int col2Id;

	private Predicate ptcPredicate = null;



	public Predicate getPtcPredicate() {
		return ptcPredicate;
	}

	public void setPtcPredicate(Predicate ptcPredicate) {
		this.ptcPredicate = ptcPredicate;
	}

	public int getCol2Id() {
		return col2Id;
	}

	public void setCol2Id(int col2Id) {
		this.col2Id = col2Id;
	}

	public int getCol1Id() {
		return col1Id;
	}

	public void setCol1Id(int colId) {
		this.col1Id = colId;
	}

	public Predicate getPredicate() {
		return predicate;
	}

	public void setPredicate(Predicate predicate) {
		this.predicate = predicate;
	}

	public enum RelationOperator {
		EQUAL("=") , LESS_THAN("<"), GREATER_THAN(">"), IN("IN") ;
		private String val;

		RelationOperator(String val) {
			this.val = val;
		}

		public String getVal() {
			return this.val;

		}
	}

	public enum Type {
		E('E') , R('R'), I('I');
		private char val;

		Type(char val) {
			this.val = val;
		}

		public char getVal() {
			return this.val;

		}
	}

	public boolean isInList() {
		return inList;
	}

	public void setInList(boolean inList) {
		this.inList = inList;
	}

	public boolean isJoin() {
		return join;
	}

	public void setJoin(boolean join) {
		this.join = join;
	}

	public char getType() {
		return type;
	}

	public void setType(char type) {
		this.type = type;
	}

	public int getCard1() {
		return card1;
	}

	public void setCard1(int card1) {
		this.card1 = card1;
	}

	public int getCard2() {
		return card2;
	}

	public void setCard2(int card2) {
		this.card2 = card2;
	}

	public double getFf1() {
		return ff1;
	}

	public void setFf1(double ff1) {
		this.ff1 = ff1;
	}

	public double getFf2() {
		return ff2;
	}

	public void setFf2(double ff2) {
		this.ff2 = ff2;
	}

	public int getSequence() {
		return sequence;
	}

	public void setSequence(int sequence) {
		this.sequence = sequence;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public RelationOperator getRelationOperator() {
		return relationOperator;
	}

	public void setRelationOperator(RelationOperator relationOperator) {
		this.relationOperator = relationOperator;
	}

	public static void printTable(DbmsPrinter out, ArrayList<Predicate> predicates) {
		DecimalFormat df = new DecimalFormat("#.###");
		int n = 1;
		out.println("--------------------------------------------------------------------------------------------------------------");
		out.println(String.format("%-20s %-7s %-7s %-7s %-8s %-8s %-7s %-25s %-25s",
				"| Predicate Table", "| Type", "| C1", "| C2", "| FF1",
				"| FF2", "| Seq", "| Text", "| Description"));
		out.println("--------------------------------------------------------------------------------------------------------------");
		for (Predicate p : predicates) {

			out.println(String.format("%-20s %-7s %-7s %-7s %-8s %-8s %-7s %-25s %-25s",
					"| PredNo " + n, "| " + p.getType(), "| " + p.getCard1(),
					"| " + p.getCard2(), "| " + df.format(p.getFf1()), "| "
							+ df.format(p.getFf2()), "| " + p.getSequence(),
							"| " + p.getText(),  "| " + p.getDescription()));
			out.println("--------------------------------------------------------------------------------------------------------------");
			n++;
			}
		}

		@Override
		public String toString() {
			return "Predicate [inList=" + inList + ", join=" + join + ", type="
					+ type + ", card1=" + card1 + ", card2=" + card2 + ", ff1="
					+ ff1 + ", ff2=" + ff2 + ", sequence=" + sequence + ", text="
					+ text + ", description=" + description + "]";
		}

	}
