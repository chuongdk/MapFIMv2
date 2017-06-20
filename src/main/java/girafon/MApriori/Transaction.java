package girafon.MApriori;

public class Transaction {	
	public final int id;
	public final int size = 0;
	
	Transaction(int id) {
		this.id = id;
	}
	
	
	@Override
	public String toString() {
		return id + " ";
	}

}
