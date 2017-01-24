package edu.umn.cs.Nebula.job;

public class TaskInfo {

    private final TaskStatus status;
    private final long updateTime;
    private final double load;

    public TaskInfo(TaskStatus status, long updateTime, double load) {
        this.status = status;
        this.updateTime = updateTime;
        this.load = load;
    }

    public TaskStatus getStatus() { return status; }
    public long getUpdateTime() { return updateTime; }
    public double getLoad() { return load; }
    
    public String getInfo() {
    	return "" + status + ":" + updateTime + ":" + load;
    }
}
