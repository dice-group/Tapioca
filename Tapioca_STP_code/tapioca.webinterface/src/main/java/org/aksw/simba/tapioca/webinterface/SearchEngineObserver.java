package org.aksw.simba.tapioca.webinterface;

import java.io.Serializable;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;

@ManagedBean(name = "searchEngineObserver")
@ViewScoped
public class SearchEngineObserver implements Serializable  {
	private static final long serialVersionUID = 1L;
	private static Integer progress = 0;  
	
    public Integer getProgress() { 
		setProgress(SearchEngineBean.getTMEngine().getWorkProgress());
        if(progress == null || progress == 0) {
            progress = 1;            
        }
        return progress;  
    }  
    
    public void setProgress(Integer value) { 
        progress = value;         
    }  

    public void start() {}

    /**
	 * when task finish
	 */
    public void onComplete() {  
        //FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO, "Progress Completed", "Progress Completed"));  
    }  

	/**
	 * if task is cancelled
	 */
    public void cancel() {  
    	//progress = null;  
    }  	
}
