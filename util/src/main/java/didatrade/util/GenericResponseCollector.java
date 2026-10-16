package didatrade.util;

import java.util.ArrayList;

public class GenericResponseCollector<T>  {
    private GenericResponseProcessor     processor;
    private ArrayList<T>                 collected_responses;
    private int                          received;
    private int                          pending;
    private boolean                      done;
	// run this function when done = true, this function should be implemented in the main loop
	private Runnable on_done;
    
	public GenericResponseCollector(ArrayList<T> responses, int maxresponses) {
	this.processor           = null;
    this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
    }

	// phase2 specific case
    public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p, Runnable on_done) {
	this.processor           = p;
    this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
	this.on_done 			 = on_done;
    }

	public GenericResponseCollector(ArrayList<T> responses, int maxresponses, GenericResponseProcessor p) {
	this.processor           = p;
    this.collected_responses = responses;
	this.received            = 0;
	this.pending             = maxresponses;
	this.done                = false;
    }

    public synchronized void addResponse(T resp) {

		// to avoid running onPhaseTwoDone more then once
		if (this.done) {
			return;
		}

		if (!this.done) {
			collected_responses.add(resp);
			if (this.processor != null)
				this.done = this.processor.onNext(this.collected_responses, resp);
		}
		this.received++;
		this.pending--;

		if (this.pending==0) {
			this.done = true;
		}

		if (this.done && on_done != null) {
			this.on_done.run();	
		}
		
		notifyAll();
    }


    public synchronized void addNoResponse() {

		if (this.done) {
			return;
		}
		
		this.pending--;
		if (this.pending==0) {
			this.done = true;
		}

		if (this.done && on_done != null) {
			this.on_done.run();
		}

		notifyAll();
    }


    public synchronized void waitForQuorum(int quorum) {
        while ((this.done == false) && (this.received < quorum)) {
            try {
				wait();
			}
			catch (InterruptedException e) {}
		}
		this.done = true;
    }

    
    public synchronized void waitUntilDone() {
        while (this.done == false) {
            try {
				wait();
			}
			catch (InterruptedException e) {}
		}
    }
}
