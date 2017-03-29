import java.io.Serializable;
import java.util.List;

public class Topic implements Serializable {
	private int id;
	private List<String> keywords;
	private String name;

	public String getName() {
		return name ;
	}

	public void setName(String name) {
		this.name = name ;
	}

	public int getId() {
		return id ;
	}

	public void setId(int id) {
		this.id = id ;
	}

}
