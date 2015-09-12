package com.latticeengines.liaison.exposed.service;

public class LiaisonResponse {
	
	private final int code;
	private final String message;
	
	public enum Report {

		SUCCESS( 0, "00 : Success" );
		
		private final int code;
		private final String message;
		
		private Report( int code , String message ) {
			this.code = code;
			this.message = message;
		}
		
		public int getCode() {
			return code;
		}
		
		public String getMessage() {
			return message;
		}
	}
	
	public LiaisonResponse( Report report ) {
		this.code = report.getCode();
		this.message = report.getMessage();
	}
	
	public int getCode() {
		return code;
	}
	
	public String getMessage() {
		return message;
	}

}
