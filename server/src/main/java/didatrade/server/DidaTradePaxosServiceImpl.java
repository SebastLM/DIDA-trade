
package didatrade.server;

import java.util.ArrayList;
import java.util.List;

import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.GenericResponseCollector;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

public class DidaTradePaxosServiceImpl extends DidaTradePaxosServiceGrpc.DidaTradePaxosServiceImplBase {
    DidaTradeServerState server_state;

    public DidaTradePaxosServiceImpl(DidaTradeServerState state) {
		this.server_state = state;
    }

    @Override
    public void phaseone(DidaTradePaxos.PhaseOneRequest request, StreamObserver<DidaTradePaxos.PhaseOneReply> responseObserver) {
		this.server_state.getDebugHandler().verifyDebug();

		boolean accepted      = false;
		int instance          = request.getInstance();
		int ballot            = request.getRequestballot();

		PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance, ballot);

		int value            = entry.command_id;
		int valballot        = entry.write_ballot;

		if (ballot >= this.server_state.getCurrentBallot()) {
			accepted = true;
			this.server_state.setCurrentBallot(ballot);
		}

		int maxballot = this.server_state.getCurrentBallot();

		DidaTradePaxos.PhaseOneReply.Builder response_builder = DidaTradePaxos.PhaseOneReply.newBuilder();
		response_builder.setServerid(this.server_state.my_id);
		response_builder.setRequestballot(ballot);
		response_builder.setAccepted(accepted);
		response_builder.setValue(value);
		response_builder.setValballot(valballot);
		response_builder.setMaxballot(maxballot);


		/*
			[acceptor] Multipaxos Phase 1
			Send everything Normal Paxos would send PLUS a log containing [instance, ballot]
			Instance values range from the instance we recived (instance) up until the last instance that we still have not decided anything

			NOTE: IMPLEMENTATION DETAIL; We dont send entries that were not accepted, even if there is some instance before that that is decided
			We expect the proposer to be smart and note that if we dont send instace X and send X+1 is because X is not accepted.
		*/
		List<PaxosInstance> entries = this.server_state.paxos_log.entriesFrom(instance);
		for (PaxosInstance e : entries) {
			if (e.write_ballot > -1) {
				DidaTradePaxos.LogEntry.Builder log_entry_builder = DidaTradePaxos.LogEntry.newBuilder();
				log_entry_builder.setInstance(e.instance_nb);
				log_entry_builder.setValue(e.command_id);
				log_entry_builder.setValballot(e.write_ballot);
				response_builder.addLog(log_entry_builder.build());
			}
		}

		DidaTradePaxos.PhaseOneReply response = response_builder.build();

		// System.out.println("Sending phase1 response: " + response);
		responseObserver.onNext(response);
		responseObserver.onCompleted();
    }

    @Override
    public void phasetwo(DidaTradePaxos.PhaseTwoRequest request, StreamObserver<DidaTradePaxos.PhaseTwoReply> responseObserver) {
		// System.out.println ("Receive phase two request: \n" + request);
		this.server_state.getDebugHandler().verifyDebug();
		int instance          = request.getInstance();
		int ballot            = request.getRequestballot();
		int value             = request.getValue();
		PaxosInstance entry   = this.server_state.paxos_log.testAndSetEntry(instance);
		boolean accepted      = false;
		boolean first_accept  = false;
		int  maxballot        = ballot;

		synchronized (entry) {
			if (ballot >= this.server_state.getCurrentBallot()) {
				accepted           = true;
				// dont notify the learners twice
				first_accept       = (entry.write_ballot != ballot);
				entry.command_id   = value;
				entry.write_ballot = ballot;
				this.server_state.setCurrentBallot(ballot);
			}
			else
				maxballot = this.server_state.getCurrentBallot();
		}

		DidaTradePaxos.PhaseTwoReply.Builder response_builder = DidaTradePaxos.PhaseTwoReply.newBuilder();
		response_builder.setInstance(instance);
		response_builder.setAccepted(accepted);
		response_builder.setServerid(this.server_state.my_id);
		response_builder.setRequestballot(ballot);
		response_builder.setMaxballot(maxballot);


		DidaTradePaxos.PhaseTwoReply response = response_builder.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();

		// Notify learners
		if (accepted == true && first_accept == true) {

			Context ctx = Context.current().fork();
			ctx.run(() -> {
				List<Integer> learners = this.server_state.scheduler.learners(ballot);
				int n_targets          = learners.size();

				DidaTradePaxos.LearnRequest.Builder learn_request_builder = DidaTradePaxos.LearnRequest.newBuilder();
				learn_request_builder.setInstance(instance);
				learn_request_builder.setValue(value);
				learn_request_builder.setBallot(ballot);

				DidaTradePaxos.LearnRequest learn_request = learn_request_builder.build();

				// System.out.println("Sending learn request: \n" + learn_request);

				System.out.println("Paxos acceptor: going to notify learners for entry " + instance + " with timestamp " + ballot + " request = " + learn_request);
				ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
				GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);;
				for (int i = 0; i < n_targets; i++) {
					CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
					this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
				}
				// System.out.println("Learn request completed for instance = " + instance);
			});
		}
    }

	@Override
	public void phaseany(DidaTradePaxos.PhaseTwoFastRequest request, StreamObserver<DidaTradePaxos.PhaseTwoFastReply> responseObserver) {
		this.server_state.getDebugHandler().verifyDebug();
		int instance          = request.getInstance();
		int ballot            = request.getRequestballot();
		boolean accepted      = false;
		int  maxballot        = ballot;

		if (ballot >= this.server_state.getCurrentBallot()) {
			accepted = true;
			this.server_state.setCurrentBallot(ballot);
			this.server_state.setFastPaxosMode(true);
			this.server_state.setFastBallot(ballot);
			this.server_state.setFastNextInstance(instance);
		}
		else {
			maxballot = this.server_state.getCurrentBallot();
		}

		DidaTradePaxos.PhaseTwoFastReply.Builder response_builder = DidaTradePaxos.PhaseTwoFastReply.newBuilder();
		response_builder.setAccepted(accepted);
		response_builder.setServerid(this.server_state.my_id);
		response_builder.setMaxballot(maxballot);

		DidaTradePaxos.PhaseTwoFastReply response = response_builder.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}


    @Override
    public void learn(DidaTradePaxos.LearnRequest request, StreamObserver<DidaTradePaxos.LearnReply> responseObserver) {
		this.server_state.getDebugHandler().verifyDebug();
		int instance         = request.getInstance();
		int ballot           = request.getBallot();
		int value            = request.getValue();

		synchronized (this) {
			PaxosInstance entry  = this.server_state.paxos_log.testAndSetEntry(instance);

			this.server_state.setCurrentBallot(ballot);

			if (this.server_state.scheduler.fastpaxos(ballot)) {
				if (ballot > entry.accept_ballot) {
					entry.accept_ballot = ballot;
					entry.fast_votes.clear();
				}

				if (ballot == entry.accept_ballot) {
					// verifys the amount of votes for the specific value and at the same time adds one
					int votes = entry.fast_votes.merge(value, 1, Integer::sum);
					System.out.println("Paxofs learner for instance " + instance + " : value " + value + " has " + votes + " fast accepts");
					if (votes >= this.server_state.fastQuorum(ballot)) {
						System.out.println("Paxos learner: waking up the main loop");
						entry.decided_value = value;
						entry.decided       = true;
						this.server_state.main_loop.wakeup();
					}
				}
			}
			else if (ballot == entry.accept_ballot) {
				entry.n_accepts++;
				System.out.println("Paxos learner for instance " + instance + " : number of accepts " +  entry.n_accepts);
				if (entry.n_accepts >= this.server_state.scheduler.quorum(ballot)) {
					System.out.println("Paxos learner: waking up the main loop");
					entry.decided = true;
					this.server_state.main_loop.wakeup();
				}
			} else if (ballot > entry.accept_ballot) {
				System.out.println("Paxos learner for instance " + instance + " : resetting ");
				entry.decided_value  = value;
				entry.accept_ballot  = ballot;
				entry.n_accepts      = 1;
			}
		}

		DidaTradePaxos.LearnReply.Builder response_builder = DidaTradePaxos.LearnReply.newBuilder();
		response_builder.setInstance(instance);
		response_builder.setBallot(ballot);

		DidaTradePaxos.LearnReply response = response_builder.build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
    }
}
