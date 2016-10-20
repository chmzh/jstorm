package cndw.jstorm;

import backtype.storm.tuple.Tuple;

public class DvDataEntry {
	private String gameId;
	private DvObject object;
	private Tuple tuple;
	private String sql;
	public Tuple getTuple() {
		return tuple;
	}
	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public String getGameId() {
		return gameId;
	}
	public void setGameId(String gameId) {
		this.gameId = gameId;
	}
	public DvObject getObject() {
		return object;
	}
	public void setObject(DvObject object) {
		this.object = object;
	}
	
	public static void main(String[] args) {
		for(int i=9;i<10000;i++){
			if(i%2==1 && i%3==0 && i%4==1 && i%5==4 && i%6==3 && i%7 == 0 && i%8==1 && i%9==0){
				System.out.println(i);
			}
		}
	}
	
}
