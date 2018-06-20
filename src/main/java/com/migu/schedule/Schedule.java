package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/*
*类名和方法不能修改
 */
public class Schedule {
	Map<Integer,String> nodeIds = new HashMap<Integer,String>();//服务节点列表(map里key是nodeId、value是taskId的字符串拼接（用静态类应该更方便，时间不够了）)
	Map taskIds = new HashMap(); //任务列表（key是taskId,value是consumption）
	Map taskIds_g = new HashMap(); //挂起任务列表（key是taskId,value是consumption）
	
	

    public int init() {
    	//清除服务节点并且清除所有任务
    	nodeIds.clear();
    	taskIds.clear();
    	taskIds_g.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
    	//添加服务节点
    	if(nodeId<0){
    		return ReturnCodeKeys.E004;
    	}else if(nodeIds.containsKey(nodeId)){
    		return ReturnCodeKeys.E005;
    	}else{
    		nodeIds.put(nodeId, "");
    		return ReturnCodeKeys.E003;
    	}
    	
    }
    
    public int registerNodeTask(int nodeId,int taskId) {
    	//往服务节点里面添加任务
    	if(nodeIds.get(nodeId)==""){
    		nodeIds.put(nodeId, taskId+"");
    	}else{
    		nodeIds.put(nodeId, nodeIds.get(nodeId)+"|"+taskId);
    	}
        return ReturnCodeKeys.E000;
    }
    
    public int unregisterNode(int nodeId) {
    	if(nodeId<=0){
    		return ReturnCodeKeys.E004;
    	}else if(!nodeIds.containsKey(nodeId)){
    		return ReturnCodeKeys.E007;
    	}else{
    		//注销服务节点
        	nodeIds.remove(nodeId);
        	//将执行中的任务移到挂起队列
        	String taskId_s=nodeIds.get(nodeId);
        	System.out.println(taskIds_g.get(taskId_s));
        	if(taskId_s!=null){
        		String[] taskId_ss = taskId_s.split("\\|");
            	for(int i=0;i<taskId_ss.length;i++){
            		taskIds_g.put(taskId_ss[i], taskIds.get(taskId_ss[i]));
                }
        	}
        	System.out.println(taskIds_g.get(taskId_s));
        	return ReturnCodeKeys.E006;
    		
    	}
    	
    	
    }


    public int addTask(int taskId, int consumption) {
    	if(taskId<=0){
    		return ReturnCodeKeys.E009;
    	}else if(taskIds.containsKey(taskId)){
    		return ReturnCodeKeys.E010;
    	}else{
    		taskIds_g.put(taskId, consumption);//挂起列表
        	taskIds.put(taskId, consumption);//所有任务列表
        	return ReturnCodeKeys.E008;
    		
    	}
    	
    }


    public int deleteTask(int taskId) {
    	if(taskId<=0){
    		return ReturnCodeKeys.E009;
    	}else if(!taskIds.containsKey(taskId)){
    		return ReturnCodeKeys.E012;
    	}else{
    		//删除任务列表中任务
        	taskIds_g.remove(taskId);
        	//删除挂起列表中任务
        	taskIds.remove(taskId);
        	//删除正在执行的任务
        	Iterator it = nodeIds.entrySet().iterator();  
        	  while (it.hasNext()) {  
        	   Map.Entry entry = (Map.Entry) it.next();  
        	   int nodeId = (Integer) entry.getKey();  
        	   String taskId_s = (String) entry.getValue();  
        	   taskId_s.replace(taskId+"", "");
        	   nodeIds.put(nodeId, taskId_s);
        	  } 
        	return ReturnCodeKeys.E011;
    		
    	}
    	
    	
    	 

    }


    public int scheduleTask(int threshold) {
    	 if (threshold<=0) { 
    		 return ReturnCodeKeys.E002;
    	} else {
    			
    			return ReturnCodeKeys.E014; 
    		
    	} 

    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
    	if (null != tasks) { 
    		tasks.clear(); 
    	for (TaskInfo item : tasks) { 
    		 TaskInfo task = new TaskInfo(); 
    		 task.setNodeId(item.getNodeId()); 
    		 task.setTaskId(item.getTaskId()); 
    		 tasks.add(task); 
    		 } 
    		 return ReturnCodeKeys.E015; 
    		} else { 
    		 return ReturnCodeKeys.E016; 
    	} 

    }

}
