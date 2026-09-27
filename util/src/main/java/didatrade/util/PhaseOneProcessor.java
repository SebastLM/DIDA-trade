package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private boolean                  accepted;
    private Map<Integer, Integer>    values;
    private Map<Integer, Integer>    valballot;
    private int                      maxballot;
    private int                      responses;
    private int                      quorum;

    public PhaseOneProcessor(ConfigurationScheduler s, int q) {
	// System.out.println("Phase 1 processor constructor with low_ballot =" + l + " high_ballot = " + h);
    this.accepted    = true;
	this.values      = new HashMap<>();
    this.valballot   = new HashMap<>();
	this.maxballot   = -1;
	this.scheduler   = s;
    this.quorum = q;
    this.responses = 0;
    }

    public boolean getAccepted() {
		return this.accepted;
    }

    public Map<Integer, Integer> getValues() {
        return this.values;
    }

    public Map<Integer, Integer> getValballot() {
	    return this.valballot;
    }

    public int getMaxballot() {
	    return this.maxballot;
    }

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response){
        this.responses++;

        if (last_response.getAccepted() == false) {
			this.accepted = false;
			this.maxballot = last_response.getMaxballot();
			return true;
		}

        for (DidaTradePaxos.LogEntry entry: last_response.getLogList()) {
            int slot   = entry.getInstance();
            int value  = entry.getValue();
            int ballot = entry.getValballot();

            if (!this.valballot.containsKey(slot) || this.valballot.get(slot) < ballot) {
                this.valballot.put(slot, ballot);
                this.values.put(slot, value);
            }
        }

        if (this.responses >= this.quorum) {
            return true;
        }

        return false;
    }
}