package didatrade.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import didatrade.DidaTradePaxos;

import didatrade.configs.ConfigurationScheduler;

public class PhaseOneProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseOneReply>  {
    private ConfigurationScheduler   scheduler;
    private boolean                  accepted;
    private Map<Integer, Integer>    values;
    private Map<Integer, Integer>    valballot;


    // Vertical paxos: phase 1 needs a majority of EACH of them
    //   A: [{0,1,2}]
    //   B: [{0,1,2,3,4}]
    //   C: [{0,1,2,3,4}, {2,3,4,5,6}]
    private List<Set<Integer>> configs   = new ArrayList<>();
    // Check who responded for the majority of EACH conf
    private Set<Integer>       responded = new HashSet<>();

    // Instance1 : ((Value, Ballot): Num_of_Votes ), ...
    private Map<Integer, Map<ValueBallot, Integer>> votes;

    private int                      maxballot;
    private int                      responses;
    private int                      quorum;
    private int                      low;
    private int                      high;


    public PhaseOneProcessor(ConfigurationScheduler s, int q, int l, int h) {
	    System.out.println("Phase 1 processor constructor with low_ballot =" + l + " high_ballot = " + h);
        this.accepted    = true;
        this.values      = new HashMap<>();
        this.valballot   = new HashMap<>();
        this.votes       = new HashMap<>();
        this.maxballot   = -1;
        this.scheduler   = s;
        this.quorum = q;
        this.low = l;
        this.high = h;
        this.responses = 0;

        for (int b = this.low; b <= this.high; b++) {
            Set<Integer> conf = new HashSet<>(scheduler.acceptors(b));
            if (!configs.contains(conf)) {
                configs.add(conf);
            }
        }
    }

    private boolean hasMajorityOfEachConfig() {
        if (configs.isEmpty()) {
            return false;
        }
        for (Set<Integer> conf : configs) {
             // how many of the servers that replied belong to this configuration
            int count = 0;
            for (int id : responded) {
                if (conf.contains(id)) {
                    count++;
                }
            }
            if (count < conf.size() / 2 + 1) {
                return false;
            }
        }
        return true;
    }

    public boolean getAccepted() {
        // The this.responses >= this.quorum was not on the base code but if we dont get a majority of responses 
        // (E.g: one acceptor lives and others are dead) then we should not be allowed to make progress
		return this.accepted && hasMajorityOfEachConfig();
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

    private record ValueBallot(int value, int ballot) {}

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseOneReply> all_responses, DidaTradePaxos.PhaseOneReply last_response){
        this.responses++;
        responded.add(last_response.getServerid());

        if (last_response.getAccepted() == false) {
			this.accepted = false;
			this.maxballot = last_response.getMaxballot();
			return true;
		}

        for (DidaTradePaxos.LogEntry entry: last_response.getLogList()) {
            int slot   = entry.getInstance();
            int value  = entry.getValue();
            int ballot = entry.getValballot();

            ValueBallot vb = new ValueBallot(value, ballot);

            if (!this.votes.containsKey(slot)) {
                this.votes.put(slot, new HashMap<>());
            }

            Map<ValueBallot, Integer> slot_votes = this.votes.get(slot);

            if (!slot_votes.containsKey(vb)) {
                slot_votes.put(vb, 1);
            } else {
                slot_votes.put(vb, slot_votes.get(vb) + 1);
            }

            if (!this.valballot.containsKey(slot) || this.valballot.get(slot) < ballot) {
                this.valballot.put(slot, ballot);
                this.values.put(slot, value);
            }
        }

        // After a quorum replied: check whether a value was chosen or not, fast-paxos proof
        if (this.hasMajorityOfEachConfig()) {
            for (Map.Entry<Integer, Integer> e : this.valballot.entrySet()) {
                int e_paxos_instance = e.getKey();
                int e_ballot         = e.getValue();

                // Search how many responses we got from the entry (e) acceptors
                Set<Integer> acceptors_e = new HashSet<>(this.scheduler.acceptors(e_ballot));
                int responses_from_this_conf = 0;
                for (int id : responded) {
                    if (acceptors_e.contains(id)) {
                        responses_from_this_conf++;
                    }
                }

                int n_acceptors       = this.scheduler.acceptors(e_ballot).size();
                int fast_quorum       = (3 * n_acceptors + 3) / 4;
                int enough_fast_votes = (fast_quorum + responses_from_this_conf) - n_acceptors;

                for (Map.Entry<ValueBallot, Integer> vote_entry : this.votes.get(e_paxos_instance).entrySet()) {
                    ValueBallot e_vb = vote_entry.getKey();
                    if (e_vb.ballot() == e_ballot && vote_entry.getValue() >= enough_fast_votes) {
                        this.values.put(e_paxos_instance, e_vb.value());
                        break;
                    }
                }
            }

            return true;
        }

        return false;
    }
}