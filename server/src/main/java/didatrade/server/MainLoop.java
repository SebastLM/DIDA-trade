package didatrade.server;

import java.util.*;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;
import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneProcessor;
import didatrade.util.PhaseTwoResponseProcessor;
import didatrade.util.PhaseTwoFastResponseProcessor;
import io.grpc.ManagedChannel;

public class MainLoop implements Runnable  {
    DidaTradeServerState server_state;

    private boolean has_work;
    private int next_log_entry;

	private Map<Integer, Integer> recovered_values = new HashMap<>();

    private List<Integer> all_participants;
    private int n_participants;
    private String[] targets;
    private ManagedChannel[] channels;
    private DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

	private int next_slot_to_dispatch;
	private int last_any_ballot;

	private final Object recovery_lock     = new Object();
	private Set<Integer> slots_we_recover  = new HashSet<>();
	private int          recovering_ballot = -1;

    public MainLoop(DidaTradeServerState state) {
		this.server_state   = state;
		this.has_work       = false;
		this.next_log_entry = -1;
		this.next_slot_to_dispatch = 0;
		this.last_any_ballot = -1;
    }

	private boolean acceptorsChanged(int cb, int ballot) {
		Set<Integer> current = new HashSet<>(this.server_state.scheduler.acceptors(ballot));
		for (int b = Math.max(cb, 0); b < ballot; b++) {
			if (!current.equals(new HashSet<>(this.server_state.scheduler.acceptors(b)))) {
				return true;
			}
		}
		return false;
	}

	private void recoverAsNewLeader(int ballot) {
		if (this.isLeader(ballot) && this.server_state.getLastPreparedBallot() < ballot) {

			int cb = this.server_state.getCompletedBallot();

			// Verify, did any ballot in [cb, ballot-1] (low, high) used a set of diff acceptors then this ballot?
			// if so we must re-send the history (state-transfer)
			boolean acceptors_changed = this.acceptorsChanged(cb, ballot);

			Map<Integer, Integer> phase_two_values = this.processPhaseOne(this.next_log_entry, ballot, cb);
				if (phase_two_values != null) {

         			// The checklist: every slot to send in phase 2 for this ballot
					Map<Integer, Integer> recovery = new HashMap<>();
					//  State-transfer!
					if (acceptors_changed) {
						for (int slot = 0; slot < this.next_log_entry; slot++) {
							recovery.put(slot, this.server_state.paxos_log.getEntry(slot).decided_value);
						}
					}
					// Pending requests may have been lost (probabily impossible but for safety)
					this.server_state.req_history.resetProposed();

                    // store the phase 1 values so on proposePending we avoid proposing them twice
					// would not affect correctness but its dumb to send them twice.
					this.recovered_values = phase_two_values;

					this.next_slot_to_dispatch = this.next_log_entry;

					int number_of_recovered_values = phase_two_values.size();
					for (int slot = this.next_log_entry; number_of_recovered_values  > 0; slot++) {
						Integer value = phase_two_values.get(slot);
						if (value == null) {
							// NO-OP!
							value = -1;
						} else {
							number_of_recovered_values --;
						}
						recovery.put(slot, value);
						this.next_slot_to_dispatch = Math.max(this.next_slot_to_dispatch, slot + 1);
					}

					synchronized (this.recovery_lock) {
						// for this ballot
						this.recovering_ballot = ballot;
						// these are the slots that need to be accepted by a majority of acceptors
						this.slots_we_recover  = new HashSet<>(recovery.keySet());
					}

					// Nothing to state-transfer, this ballot is completed
					if (recovery.isEmpty()) {
						this.server_state.setCompletedBallot(ballot);
					}

					// Schedule A or B: never a state transfer, so this sends only the phase 1 values.
					// Schedule C: if the acceptor-set changed since cb, it also re-sends every decided slot in [0, next_log_entry) so the new acceptors store the history.
					for (Map.Entry<Integer, Integer> r : recovery.entrySet()) {
						this.processPhaseTwo(r.getValue(), ballot, r.getKey());
					}
				}
			}
	}

	private void startFastRound(int ballot) {
		// Dont start a FR if you are already in a fast round.
		if (this.last_any_ballot < ballot) {
			this.last_any_ballot = ballot;
			this.processAny(ballot, this.next_slot_to_dispatch);
		}
	}

	private boolean isLeader(int ballot) {
		return this.server_state.scheduler.leader(ballot) == this.server_state.my_id;
	}

	private void proposePending(int ballot) {
		RequestRecord request_record = this.server_state.req_history.getFirstNotProposed();
		while (request_record != null && this.server_state.getCurrentBallot() == ballot) {

			request_record.setProposed(true);

			if (!this.recovered_values.containsValue(request_record.getId())) {
				this.processPhaseTwo(request_record.getId(), ballot, this.next_slot_to_dispatch);
				this.next_slot_to_dispatch++;
			}
			request_record = this.server_state.req_history.getFirstNotProposed();
		}

	}

	private void executeDecided(PaxosInstance entry) {
		synchronized (this.server_state.execution_lock) {
			this.server_state.setLastInstance(this.next_log_entry);

			if (entry.decided_value != -1) {
				this.execute_tx(entry.decided_value);
			}
		}

		this.recovered_values.remove(this.next_log_entry);
	}

    public void run() {
		/*
			TODO: Refactor this to include FP.
			Main loop that runs the Multi-Paxos core:

			When this server becomes leader of a ballot, run() first runs Phase 1 (once per ballot) to
			learn what the acceptors already accepted from next_log_entry onward.

			It then runs Phase 2 to re-propose each recovered value in its own instance, filling gaps
			with no-ops (-1).

			After that, it runs Phase 2 for new client requests, each in the next free instance
			(next_slot_to_dispatch).

			Instances are still executed in order: run() only applies next_log_entry once it is decided.

			!! Why Phase 2 does not wait:

				Multi-Paxos: run() keeps starting Phase 2 for the next requests, without waiting for
				earlier instances to be accepted. Several instances are in flight at once.

				The result is handled by the gRPC thread that delivers the last reply needed:
				(or raises the ballot on rejection) and wakes run().
				it calls onPhaseTwoDone(), which marks the instance decided
		*/
		while (true) {

			this.next_log_entry++;
			PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(next_log_entry);

			while(next_entry.decided == false) {

				int ballot = this.server_state.getCurrentBallot();
				this.recoverAsNewLeader(ballot);

				if (next_entry.decided == false && this.isLeader(ballot) && this.server_state.getLastPreparedBallot() == ballot) {

					if (this.server_state.scheduler.fastpaxos(ballot)) {
						// run fast paxos
						this.startFastRound(ballot);
					} else {
						// run multi-paxos
						this.proposePending(ballot);
					}
				}

				this.fastAcceptPending();
				if (next_entry.decided == false) {
					System.out.println("Entry not decided: waiting");
					this.waitForWork();
				}
			}

			System.out.println("Log entry with number " + this.next_log_entry + " has been decided with command id = "+ next_entry.command_id);
			this.executeDecided(next_entry);
		}
	}

	private void fastAcceptPending() {
		while (this.server_state.inFastMode()) {
			RequestRecord request = this.server_state.req_history.getFirstNotFastVoted();
			if (request == null)
				return;
			request.setFastVoted(true);

			int ballot;
			int slot;

			synchronized (this.server_state) {
				if (!this.server_state.inFastMode()) {
					request.setFastVoted(false);
					return;
				}
				ballot = this.server_state.getFastBallot();

				PaxosInstance entry;
				while (true) {
					slot  = this.server_state.nextFastInstance();
					entry = this.server_state.paxos_log.testAndSetEntry(slot);

					synchronized (entry) {
						if (entry.write_ballot != ballot && !entry.decided) {
							entry.command_id   = request.getId();
							entry.write_ballot = ballot;
							break;
						}
					}
				}
			}

			System.out.println("Fast accept: request " + request.getId() + " in slot " + slot + " ballot " + ballot);
			this.sendLearn(slot, ballot, request.getId());
		}
	}

	private void sendLearn(int instance, int ballot, int value) {
			List<Integer> learners = this.server_state.scheduler.learners(ballot);
			int n_targets          = learners.size();

			DidaTradePaxos.LearnRequest learn_request = DidaTradePaxos.LearnRequest.newBuilder()
					.setInstance(instance)
					.setBallot(ballot)
					.setValue(value)
					.setFast(true)
					.build();

			ArrayList<DidaTradePaxos.LearnReply> learn_responses = new ArrayList<DidaTradePaxos.LearnReply>();
			GenericResponseCollector<DidaTradePaxos.LearnReply> learn_collector = new GenericResponseCollector<DidaTradePaxos.LearnReply>(learn_responses, n_targets);
			for (int i = 0; i < n_targets; i++) {
					CollectorStreamObserver<DidaTradePaxos.LearnReply> learn_observer = new CollectorStreamObserver<DidaTradePaxos.LearnReply>(learn_collector);
					this.server_state.async_stubs[learners.get(i)].learn(learn_request, learn_observer);
			}
	}

	public synchronized void wakeup() {
		this.has_work = true;
		notify();
	}

	private synchronized void waitForWork() {
		while (this.has_work == false) {
			try {
				wait();
			} catch (InterruptedException e) {
				System.out.println("Waited interrupted" + e.getMessage());
			}
		}
		this.has_work = false;
	}

	Map<Integer, Integer> processPhaseOne(int entry_number, int ballot, int completed_ballot) {

		// Vertical Paxos I: read every configuration in [completed_ballot, ballot - 1]
		int           low_ballot  = Math.max(completed_ballot, 0);
		int           high_ballot = ballot - 1;
		List<Integer> acceptors;
		if (ballot == 0 || low_ballot > high_ballot) {
			// no previous ballot to read: use this ballot's configuration (processor range must match)
			low_ballot  = ballot;
			high_ballot = ballot;
			acceptors = this.server_state.scheduler.acceptors(ballot);
		} else {
			acceptors = this.server_state.scheduler.acceptorsinrange(low_ballot, high_ballot);
		}

		int           quorum      = this.server_state.scheduler.quorum(ballot);
		int           n_acceptors = acceptors.size();

		// Paxos Phase One
		System.out.println("Going to run paxos phase 1");

		// send phase1
		DidaTradePaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaTradePaxos.PhaseOneRequest.newBuilder();
		phase_one_request_builder.setInstance(entry_number);
		phase_one_request_builder.setRequestballot(ballot);

		DidaTradePaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();
		System.out.println("Going to send phase 1" + phase_one_request);

		PhaseOneProcessor phase_one_processor = new PhaseOneProcessor(this.server_state.scheduler, quorum, low_ballot, high_ballot);

		ArrayList<DidaTradePaxos.PhaseOneReply> phase_one_responses = new ArrayList<DidaTradePaxos.PhaseOneReply>();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector = new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(phase_one_responses, n_acceptors, phase_one_processor);

		for (int i = 0; i < n_acceptors; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> phase_one_observer  = new CollectorStreamObserver<DidaTradePaxos.PhaseOneReply>(phase_one_collector);
			this.server_state.async_stubs[acceptors.get(i)].phaseone(phase_one_request, phase_one_observer);
		}

		/*
			!! Why waitUntilDone() and not waitForQuorum(): fast paxos waitForrequires 3/2 * f + 1 quorums, so in our case, since we have 5 acceptors, we will need sized 4 quorums

			We still only wait for a quorum PhaseOneProcessor.onNext() returns true as soon as a quorum 
			has replier or as soon as one acceptor rejects.

			waitForQuorum() is not safe here:
				It also returns when every RPC failed, even though fewer than a quorum answered.
				The caller cannot tell this apart from success.

			So we always return, and PhaseOneProcessor.getAccepted() checks both no rejection and
			responses >= quorum. If there is no quorum, we fail this round and retry/sleep below. That keeps
			safety (no progress without a majority) and liveness.
        */
		phase_one_collector.waitUntilDone();

		// Phase one didnt work
		if (phase_one_processor.getAccepted() == false) {
			int maxballot = phase_one_processor.getMaxballot();
			if (maxballot > this.server_state.getCurrentBallot()) {
				this.server_state.setCurrentBallot(maxballot);
			}
			return null;
		}

		// Phase one worked
		this.server_state.setLastPreparedBallot(ballot);
		return phase_one_processor.getValues();
	}

	// Returns true if the value was accepted by a quorum
	void processAny(int ballot, int entry_number) {
		System.out.println("Going to run fast paxos round, sending ANY to every acceptor");

		List<Integer> acceptors        = this.server_state.scheduler.acceptors(ballot);
		int           quorum           = this.server_state.scheduler.quorum(ballot);
		int           n_acceptors      = acceptors.size();

		// send ANY
		DidaTradePaxos.PhaseTwoFastRequest.Builder phase_two_fast_request = DidaTradePaxos.PhaseTwoFastRequest.newBuilder();
		phase_two_fast_request.setInstance(entry_number);
		phase_two_fast_request.setRequestballot(ballot);

		PhaseTwoFastResponseProcessor phase_two_fast_processor = new PhaseTwoFastResponseProcessor(quorum);

		System.out.println("Calling peers with phase_two_fast_request = " + phase_two_fast_request);
		ArrayList<DidaTradePaxos.PhaseTwoFastReply> phase_two_fast_responses = new ArrayList<DidaTradePaxos.PhaseTwoFastReply>();
		GenericResponseCollector<DidaTradePaxos.PhaseTwoFastReply> phase_two_fast_collector = new GenericResponseCollector<DidaTradePaxos.PhaseTwoFastReply>(phase_two_fast_responses, n_acceptors, phase_two_fast_processor, () -> this.onPhaseTwoFastDone(phase_two_fast_processor, ballot, entry_number));

		for (int i = 0; i < n_acceptors  ; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseTwoFastReply> phase_two_fast_observer = new CollectorStreamObserver<DidaTradePaxos.PhaseTwoFastReply>(phase_two_fast_collector);
			this.server_state.async_stubs[acceptors.get(i)].phaseany(phase_two_fast_request.build(), phase_two_fast_observer);
		}
		System.out.println("Waiting for responses...");
		return;
	}

	// Returns true if the value was accepted by a quorum
	void processPhaseTwo(int value, int ballot, int entry_number) {

		System.out.println("Going to run paxos phase 2");
		PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(entry_number);

		List<Integer> acceptors        = this.server_state.scheduler.acceptors(ballot);
		int           quorum           = this.server_state.scheduler.quorum(ballot);
		int           n_acceptors      = acceptors.size();

		// send phase2
		DidaTradePaxos.PhaseTwoRequest.Builder phase_two_request = DidaTradePaxos.PhaseTwoRequest.newBuilder();
		phase_two_request.setInstance(entry_number);
		phase_two_request.setRequestballot(ballot);
		phase_two_request.setValue(value);

		PhaseTwoResponseProcessor phase_two_processor = new PhaseTwoResponseProcessor(quorum);

		System.out.println("Calling peers with phase_two_request = " + phase_two_request);
		ArrayList<DidaTradePaxos.PhaseTwoReply> phase_two_responses = new ArrayList<DidaTradePaxos.PhaseTwoReply>();
		GenericResponseCollector<DidaTradePaxos.PhaseTwoReply> phase_two_collector = new GenericResponseCollector<DidaTradePaxos.PhaseTwoReply>(phase_two_responses, n_acceptors, phase_two_processor, () -> this.onPhaseTwoDone(phase_two_processor, value, ballot, entry_number));

		for (int i = 0; i < n_acceptors  ; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply> phase_two_observer = new CollectorStreamObserver<DidaTradePaxos.PhaseTwoReply>(phase_two_collector);
			this.server_state.async_stubs[acceptors.get(i)].phasetwo(phase_two_request.build(), phase_two_observer);
		}

		System.out.println("Waiting for responses...");
		return;
	}

	private void onPhaseTwoDone(PhaseTwoResponseProcessor phase_two_processor, int value, int ballot, int entry_number) {
		if (phase_two_processor.getAccepted() == false) {
			this.server_state.setCurrentBallot(phase_two_processor.getMaxballot());
			System.out.println("Paxos phase 2 ended with ballot_aborted for entry: " + entry_number + "ballot: " + ballot);

			RequestRecord request = this.server_state.req_history.getIfPending(value);
			if (request != null) {
				request.setProposed(false);
			}
			if (phase_two_processor.getMaxballot() > ballot) {
				wakeup();
			}
			return;
		} else {
			PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(entry_number);
			synchronized (entry) {
				entry.command_id = value;
				entry.decided_value = value;
				entry.decided = true;
			}

			boolean completed = false;
			synchronized (this.recovery_lock) {
				if (ballot == this.recovering_ballot
						&& this.slots_we_recover.remove(entry_number)
						&& this.slots_we_recover.isEmpty()) {
					completed = true;
				}
			}
			if (completed) {
				this.server_state.setCompletedBallot(ballot);
			}

			wakeup();
			return;
		}
    }

	private void onPhaseTwoFastDone(PhaseTwoFastResponseProcessor phase_two_fast_processor, int ballot, int entry_number) {
		if (phase_two_fast_processor.getAccepted() == false) {
			this.server_state.setCurrentBallot(phase_two_fast_processor.getMaxballot());
			System.out.println("Fast Paxos phase 2 ended with ballot_aborted for entry: " + entry_number + "ballot: " + ballot);
			if (phase_two_fast_processor.getMaxballot() > ballot) {
				wakeup();
			}
			return;
		} else {
			wakeup();
      		System.out.println("Fast Paxos ANY accepted for ballot " + ballot + " from entry " + entry_number);
			return;
		}
    }

	public Boolean execute_tx(int command_id) {

			RequestRecord record = this.server_state.req_history.getIfPending(command_id);

			/*
			Make execution idempotent by skiping commands that were already executed.

			Multipaxos "allows" this to happen, was explain in classes that either the app should address it or 
			the consesus implementation address it, we decided to address it.
			*/ 
			if (this.server_state.req_history.getIfProcessed(command_id) != null) {
				System.out.println("This command: " + command_id + " was already executed.");
				return false;
			}

			while (record == null) {
				waitForWork();
				record = this.server_state.req_history.getIfPending(command_id);
			}

			// exec request in entry
			DidaTradeCommand command = record.getRequest();
			boolean result = false;
			int balance = 0;

			DidaTradeAction action = command.getAction();

			System.out.println("Action  = " + action);
			switch (action) {
				case DidaTradeAction.POPULATE:
					result = this.server_state.trade_manager.populate(command.getQuantity());
					break;
				case DidaTradeAction.ADDUSER:
					result = this.server_state.trade_manager.add_user(command.getUserId(), command.getQuantity(), command.getStock());
					break;
				case DidaTradeAction.SELL:
					result = this.server_state.trade_manager.sell(command.getUserId(), command.getQuantity());
					break;
				case DidaTradeAction.BUY:
					result = this.server_state.trade_manager.acquire(command.getUserId(), command.getQuantity());
					break;
				case DidaTradeAction.BALANCE:
					balance = this.server_state.trade_manager.balance(command.getUserId());
					command.setQuantity(balance);
					result = (balance != -1);
					break;
				case DidaTradeAction.DUMP:
					this.server_state.trade_manager.dump();
					result = true;
					break;

				default:
					result = false;
					System.err.println("*** Unknown command ****");
					break;
			}

			// sending response
			System.out.println("Setting response for command with id = " + command_id + " with result = " + result);
			record.setResponse(result);
			this.server_state.req_history.moveToProcessed(record.getId());
			return true;
		}

	public void recover(int instance) { // instance = first instance needed
		DidaTradePaxos.StateTransferRequest stateTransferRequest = DidaTradePaxos.StateTransferRequest.newBuilder()
				.setInstance(instance)
				.build();

		List<Integer> learners = this.server_state.scheduler.learners(this.server_state.getCurrentBallot());
		int n_targets = 	learners.size() - 1; // removing this node from counting (all nodes are learners)

		ArrayList<DidaTradePaxos.StateTransferReply> stateTransferResponses = new ArrayList<DidaTradePaxos.StateTransferReply>();
		GenericResponseCollector<DidaTradePaxos.StateTransferReply> stateTransferResp_collector = new GenericResponseCollector<DidaTradePaxos.StateTransferReply>(stateTransferResponses, n_targets);

		for (int id : learners) {
			if (id == this.server_state.my_id) continue;
			CollectorStreamObserver<DidaTradePaxos.StateTransferReply> observer = new CollectorStreamObserver<>(stateTransferResp_collector);
			this.server_state.async_stubs[id].stateTransfer(stateTransferRequest, observer);
		}
		stateTransferResp_collector.waitUntilDone();

		DidaTradePaxos.StateTransferReply best = null; // find the largest response (as it already contains all others)
		for (DidaTradePaxos.StateTransferReply stateTransferReply : stateTransferResponses) {
			if (best == null || stateTransferReply.getRecoverCount() > best.getRecoverCount())
				best = stateTransferReply;
		}
		if (best == null || best.getRecoverCount() == 0) {
			return;
		}

		for (int i = 0; i < best.getRecoverCount(); i++) {
			DidaTradePaxos.PerInstance instance_x = best.getRecover(i);
			int entry_number = instance + i;

			// add the record to the pending in req_history (if it isn't already there)
			if (instance_x.getReqid() != -1 && this.server_state.req_history.getIfExists(instance_x.getReqid()) == null) {
				DidaTradeAction action = DidaTradeAction.valueOf(instance_x.getAction().name());
				DidaTradeCommand command = new DidaTradeCommand(action, instance_x.getUserid(), instance_x.getQuantity(), instance_x.getStock());
				RequestRecord record = new RequestRecord(instance_x.getReqid(), command);
				record.setProposed(true);

				this.server_state.req_history.addToPending(instance_x.getReqid(), record);
			}

			// add the instance to the log
			PaxosInstance p_instance = this.server_state.paxos_log.testAndSetEntry(entry_number);
			p_instance.decided_value = instance_x.getReqid();
			p_instance.decided = true;
		}
		this.wakeup();
	}
}