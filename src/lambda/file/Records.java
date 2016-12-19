package lambda.file;

import java.util.ArrayList;
import java.util.List;

public class Records {

	private List<Record> records;
	
	public Records(){
	}
	
	public final void addRecords(final Record record){
		if(this.records == null){
			this.records = new ArrayList<>();
		}
		this.records.add(record);
	}

	public final List<Record> getRecords() {
		return records;
	}

	public final void setRecords(List<Record> data) {
		this.records = data;
	}
	
}
