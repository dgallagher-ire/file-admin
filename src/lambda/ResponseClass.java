package lambda;

public class ResponseClass {

	private String response;
	
	public ResponseClass(){
	}
	
	public ResponseClass(final String response){
		this.response = response;
	}

	public final String getResponse() {
		return response;
	}

	public final void setResponse(String response) {
		this.response = response;
	}
}
