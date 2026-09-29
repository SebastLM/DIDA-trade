package didatrade.server;

import java.util.*;
import java.util.concurrent.*;
import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import didatrade.util.GenericResponseCollector;
import didatrade.util.CollectorStreamObserver;
import didatrade.util.PhaseOneProcessor;
import didatrade.util.PhaseTwoResponseProcessor;

import didatrade.configs.ConfigurationScheduler;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


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
    private final Semaphore window = new Semaphore(6); // window size 6

    public MainLoop(DidaTradeServerState state) {
		this.server_state   = state;
		this.has_work       = false;
		this.next_log_entry = -1;
		this.next_slot_to_dispatch = 0;
    }

    public void run() {
		/*
		Main loop that runs MultiPaxos core:
		
		
		*/
		while (true) {

			this.next_log_entry++;
			PaxosInstance next_entry = this.server_state.paxos_log.testAndSetEntry(next_log_entry);
			RequestRecord record = this.server_state.req_history.getFirstPending();

			while(next_entry.decided == false) {

				if (record == null) {
					record = this.server_state.req_history.getFirstPending();
				}

				int ballot = this.server_state.getCurrentBallot();
				if ((this.server_state.scheduler.leader(ballot) == this.server_state.my_id) && this.server_state.getLastPreparedBallot() < ballot) {
					Map<Integer, Integer> phase_two_values = this.processPhaseOne(this.next_log_entry, ballot);
					if (phase_two_values != null) {
						this.recovered_values = phase_two_values;
						this.next_slot_to_dispatch = Math.max(this.next_slot_to_dispatch, this.next_log_entry);

						int recover = phase_two_values.size();
						for (int slot = this.next_log_entry; recover > 0; slot++) {
							Integer value = phase_two_values.get(slot);
							if (value == null) {
								value = -1;
							} else {
								recover--;
							}
							this.processPhaseTwo(value, ballot, slot);
							this.next_slot_to_dispatch = Math.max(this.next_slot_to_dispatch, slot + 1);
						}
					}
				}

				if ((next_entry.decided == false && this.server_state.scheduler.leader(ballot) == this.server_state.my_id) && this.server_state.getLastPreparedBallot() == ballot) {
					RequestRecord request = this.server_state.req_history.getFirstNotProposed();
					while (request != null && this.server_state.getCurrentBallot() == ballot) {
						request.setProposed(true);
						if (!this.recovered_values.containsValue(request.getId())) {
							this.processPhaseTwo(request.getId(), ballot, this.next_slot_to_dispatch);
							this.next_slot_to_dispatch++;
						}
						request = this.server_state.req_history.getFirstNotProposed();
					}
				}

				if (next_entry.decided == false) {
					System.out.println("Entry not decided: waiting");
					this.waitForWork();

				}
			}

			System.out.println("Log entry with number " + this.next_log_entry + " has been decided with command id = "+ next_entry.command_id);
			if (next_entry.command_id != -1) {
				this.execute_tx(next_entry.command_id);
			}
			this.recovered_values.remove(this.next_log_entry);
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

	Map<Integer, Integer> processPhaseOne(int entry_number, int ballot) {

		List<Integer> acceptors        = this.server_state.scheduler.acceptors(ballot);
		int           quorum           = this.server_state.scheduler.quorum(ballot);
		int           n_acceptors      = acceptors.size();

		// Paxos Phase One
		System.out.println("Going to run paxos phase 1");

		// send phase1
		DidaTradePaxos.PhaseOneRequest.Builder phase_one_request_builder = DidaTradePaxos.PhaseOneRequest.newBuilder();
		phase_one_request_builder.setInstance(entry_number);
		phase_one_request_builder.setRequestballot(ballot);

		DidaTradePaxos.PhaseOneRequest phase_one_request = phase_one_request_builder.build();
		System.out.println("Going to send phase 1" + phase_one_request);

		PhaseOneProcessor phase_one_processor = new PhaseOneProcessor(this.server_state.scheduler, quorum);

		ArrayList<DidaTradePaxos.PhaseOneReply> phase_one_responses = new ArrayList<DidaTradePaxos.PhaseOneReply>();
		GenericResponseCollector<DidaTradePaxos.PhaseOneReply> phase_one_collector = new GenericResponseCollector<DidaTradePaxos.PhaseOneReply>(phase_one_responses, n_acceptors, phase_one_processor);

		for (int i = 0; i < n_acceptors; i++) {
			CollectorStreamObserver<DidaTradePaxos.PhaseOneReply> phase_one_observer  = new CollectorStreamObserver<DidaTradePaxos.PhaseOneReply>(phase_one_collector);
			this.server_state.async_stubs[acceptors.get(i)].phaseone(phase_one_request, phase_one_observer);
		}

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
	void processPhaseTwo(int value, int ballot, int entry_number) {
		try {
            window.acquire();
        } catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

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
			window.release();
			return;
		} else {
			PaxosInstance entry = this.server_state.paxos_log.testAndSetEntry(entry_number);
			synchronized (entry) {
				entry.command_id = value;
				entry.decided = true;
			}
			window.release();
			wakeup();
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
}