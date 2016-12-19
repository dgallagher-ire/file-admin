package lambda;

public class RequestClass {

	private String action;
	private String bucket;
	private Boolean live;
	private Integer files;
	private Integer batch;
	private String redShift;
	
	public RequestClass(){
		
	}
	
	public final String toString() {
		return 
				this.getAction() + ":" + 
				this.getBucket() + ":" + 
				this.getLive() + ":" + 
				this.getFiles() + ":" +
				this.getBatch() + ":" +
				this.getRedShift();
	}
	
	public final String getAction() {
		return action;
	}
	public final void setAction(String action) {
		this.action = action;
	}
	public final String getBucket() {
		return bucket;
	}
	public final void setBucket(String Bucket) {
		this.bucket = Bucket;
	}
	public final Boolean getLive() {
		return live;
	}
	public final void setLive(Boolean Live) {
		this.live = Live;
	}
	public final Integer getFiles() {
		return files;
	}
	public final void setFiles(Integer Files) {
		this.files = Files;
	}
	public final Integer getBatch() {
		return batch;
	}
	public final void setBatch(Integer Batch) {
		this.batch = Batch;
	}
	public final String getRedShift() {
		return redShift;
	}
	public final void setRedShift(String RedShift) {
		this.redShift = RedShift;
	}
	
	
}
