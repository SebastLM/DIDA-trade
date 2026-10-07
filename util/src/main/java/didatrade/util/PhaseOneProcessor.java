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

    // Instance1 : ((Value, Ballot): Num_of_Votes ), ...
    private Map<Integer, Map<Map<Integer, Integer>, Integer>> votes;

    private int                      maxballot;
    private int                      responses;
    private int                      quorum;

    public PhaseOneProcessor(ConfigurationScheduler s, int q) {
	    // System.out.println("Phase 1 processor constructor with low_ballot =" + l + " high_ballot = " + h);
        this.accepted    = true;
        this.values      = new HashMap<>();
        this.valballot   = new HashMap<>();
        this.votes       = new HashMap<>();
        this.maxballot   = -1;
        this.scheduler   = s;
        this.quorum = q;
        this.responses = 0;
    }

    public boolean getAccepted() {
        // The this.responses >= this.quorum was not on the base code but if we dont get a majority of responses 
        // (E.g: one acceptor lives and others are dead) then we should not be allowed to make progress
		return this.accepted && this.responses >= this.quorum;
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

            if (!this.votes.containsKey(slot)) {
                
                // Create (A, 10)
                Map<integer, integer> tuple_value_ballot = new HashMap<>();
                tuple_value_ballot.put(value, ballot);

                // Create ((A,10): 0)
                Map<Map<integer, integer>, integer> tuple_value_ballot_votes = new Hashmap<>();
                tuple_value_ballot_votes.put(tuple_value_ballot, 0);

                // Add slott, ((A,10): 0) to votes
                Map<Integer, Map<Map, integer> value_ballot_i = new HashMap<>();
                this.votes.put(slot, tuple_value_ballot_votes);
            } else {
                Map<Integer, Integer> tuple_value_ballot = new HashMap<>();
                tuple_value_ballot.put(ballot, value);

                Map<Integer, Integer> tuples_value_ballots = this.votes.get(tuple_value_ballot);
                if (tuple_value_ballot.containsKey())
            }


            if (!this.votes.containsKey(v_entry)) {
                this.votes.put(v_entry, 0);
            } else {
                int vote = votes.get(v_entry)++;
                this.votes.put(v_entry, votes.get(v_entry)++);
            }


            // HashMap<Integer, Integer> max_value_k;

            // for (HashMap<Integer, Integer> key : v_entry.keySet()) {
            //     int val = v_entry.get(key);
            //     if (val > max_value) {
            //         max_value = val;
            //         max_value_k = key;
            //     }
            // }

            if (!this.valballot.containsKey(slot) || this.valballot.get(slot) < ballot) {
                this.valballot.put(slot, ballot);
                this.values.put(slot, value);
            }
        }

        if (this.responses >= this.quorum) {
            n_acceptors = this.scheduler.acceptors(valballot.get(slot)).size()
            fast_quorum = (3 * n_acceptors + 3)/4
            fast_votes = fast_quorum + this.quorum - n_acceptors;
            
            for (Map.Entry<Integer, Integer> entry : this.valballot.entrySet()) {
                int slot = entry.getKey();
                int ballot = entry.getValue();

                for (Map.Entry<Map<Integer, Integer>, Integer> vote_entry : this.votes.get(slot).entrySet()) {
                    if (vote_entry.getKey().containsKey(ballot) && vote_entry.getValue() >= fast_votes) {
                        this.values.put(slot, vote_entry.getKey().get(ballot));
                    }
                }
            }
            
            return true;
        }

        return false;
    }
}