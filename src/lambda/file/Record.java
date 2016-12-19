package lambda.file;

public class Record {

	private Integer id;
	private String Bucket;
	private Boolean Live;
	private Integer Files;
	private Integer Batch;
	private String RedShift;
	
	public Record(){
	}

	public final String getBucket() {
		return Bucket;
	}

	public final void setBucket(String bucket) {
		Bucket = bucket;
	}

	public final Boolean getLive() {
		return Live;
	}

	public final void setLive(Boolean live) {
		Live = live;
	}

	public final Integer getFiles() {
		return Files;
	}

	public final void setFiles(Integer files) {
		Files = files;
	}

	public final Integer getBatch() {
		return Batch;
	}

	public final void setBatch(Integer batch) {
		Batch = batch;
	}

	public final String getRedShift() {
		return RedShift;
	}

	public final void setRedShift(String redShift) {
		RedShift = redShift;
	}

	public final Integer getId() {
		return id;
	}

	public final void setId(Integer id) {
		this.id = id;
	}
	
	
	
}
