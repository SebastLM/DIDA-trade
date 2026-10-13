package didatrade.server;


public class RequestRecord {
    private int requestid;
    private DidaTradeCommand request;
    private boolean response_available;
    private boolean response_value;

    // Multipaxos: locally track what reqs were send in phase2
    private boolean proposed = false;
    // Fastpaxos: locally track what reqs were fast accepted
    private boolean fast_voted = false;

    public RequestRecord(int id) {
	    this.requestid = id;
        this.request = null;
	    this.response_available = false;
	    this.response_value = false;
    }

    public RequestRecord(int id, DidaTradeCommand rq) {
	    this.requestid = id;
        this.request = rq;
	    this.response_available = false;
 	    this.response_value = false;
    }

    // Getter and Setter methods for all fields
    public DidaTradeCommand getRequest() {
        return this.request;
    }

    public int getId() {
        return this.requestid;
    }

    public void setRequest(DidaTradeCommand rq) {
        this.request = rq;
    }

    public boolean getResponseValue() {
        return this.response_value;
    }

    public synchronized boolean isFastVoted() {
        return this.fast_voted;
    }

    public synchronized void setFastVoted(boolean f) {
        this.fast_voted = f;
    }

    public synchronized boolean isProposed() {
	    return this.proposed;
    }

    public synchronized void setProposed(boolean p) {
        this.proposed = p;
    }

    public synchronized void setResponse(boolean resp) {
        this.response_value = resp;
	this.response_available = true;
	this.notifyAll();
    }
   
    public synchronized boolean waitForResponse() {
        while (this.response_available == false) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
	return this.response_value;
    }
}
