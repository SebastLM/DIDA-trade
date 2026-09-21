package didatrade.util;

import java.util.ArrayList;
import java.util.Hashtable;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private boolean                  accepted;
    private int                      value;
    private int                      valballot;
    private int                      maxballot;
    private int                      low_ballot;
    private int                      high_ballot;
    private int                      responses;
    private int                      quorum;
   
    
    public PhaseOneProcessor (ConfigurationScheduler s, int l, int h, int q) {
	// System.out.println("Phase 1 processor constructor with low_ballot =" + l + " high_ballot = " + h);
    this.accepted    = true;
	this.value       = -1;
	this.valballot   = -1;
	this.maxballot   = -1;
	this.low_ballot  = l;
	this.high_ballot = h;
	this.scheduler   = s;
    this.quorum = q;
    this.responses = 0;
    }

    public boolean getAccepted() {
	    return this.accepted;
    }
    
    public int getValue() {
	    return this.value;
    }
    
    public int getValballot() {
	    return this.valballot;
    }
    
    public int getMaxballot() {
	    return this.maxballot;
    }
    
    /*
    message PhaseOneReply {
        int32 instance        = 1;
        int32 serverid        = 2;
        int32 requestballot	= 3;
        bool  accepted 	= 4;
        int32 value           = 5;
        int32 valballot       = 6;
        int32 maxballot       = 7;
    }
    */
    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response){
        this.responses++;

        if (last_response.getAccepted() == false) {
			this.accepted = false;
			this.maxballot = last_response.getMaxballot();
			return true;
		}

        if (last_response.getValballot() > this.valballot) {
            this.valballot = last_response.getValballot() ;
            this.value = last_response.getValue();
        }

        if (this.responses > this.quorum) {
            return true;
        }
    
        return false;
    }
}
