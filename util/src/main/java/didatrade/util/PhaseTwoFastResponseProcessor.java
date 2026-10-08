package didatrade.util;

import java.util.ArrayList;

import didatrade.DidaTradePaxos;

public class PhaseTwoFastResponseProcessor extends GenericResponseProcessor<DidaTradePaxos.PhaseTwoFastReply>  {
    private boolean accepted;
    private int     maxballot;
    private int     quorum;
    private int     responses;

    public PhaseTwoFastResponseProcessor (int q) {
		this.accepted  = true;
		this.maxballot = 0;
		this.quorum    = q;
		this.responses = 0;
    }

    public boolean getAccepted() {
		return this.accepted && this.responses >= this.quorum;
    }

    public int getMaxballot() {
		return this.maxballot;
    }

    public synchronized boolean onNext(ArrayList<DidaTradePaxos.PhaseTwoFastReply> all_responses, DidaTradePaxos.PhaseTwoFastReply last_response){
		this.responses++;
		if (last_response.getAccepted() == false) {
			this.accepted = false;
			if (last_response.getMaxballot() > this.maxballot)
				this.maxballot = last_response.getMaxballot();
			return true;
		}
		else if (responses >= quorum)
			return true;
		else
			return false;
    }
}
