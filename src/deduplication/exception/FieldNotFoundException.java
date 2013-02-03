package deduplication.exception;

/**
 * @author Paulo Fernando (pf@paulofernando.net.br)
 */
public class FieldNotFoundException extends Exception {

	private static final long serialVersionUID = 1546698115254870158L;
	
	public FieldNotFoundException() {
		super("Field not found");
	}

}
