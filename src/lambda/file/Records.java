package lambda.file;

import java.util.ArrayList;
import java.util.List;

public class Records {

	private List<Record> data;
	
	public Records(){
	}
	
	public final void addRecord(final Record record){
		if(this.data == null){
			this.data = new ArrayList<>();
		}
		this.data.add(record);
	}

	public final List<Record> getData() {
		return data;
	}

	public final void setData(List<Record> data) {
		this.data = data;
	}
	
}
