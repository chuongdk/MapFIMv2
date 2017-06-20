package girafon.MApriori;



public class Item {
	  public final int id;
	  public final int support;
	  
	  public Item(int id, int support) {
		  	this.id = id;
		  	this.support = support;
		  }
		  
	  @Override
	  public String toString() {
	    return id + " (" + support + ")";
	  }
	  
	  @Override
	  public int hashCode() {
	    return id;
	  }		  
		  
	
}
