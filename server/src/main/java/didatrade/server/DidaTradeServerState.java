package didatrade.server;

import java.util.*;

import didatrade.core.*;
import didatrade.configs.*;
import didatrade.configs.ConfigurationScheduler;

import didatrade.DidaTradeMain;
import didatrade.DidaTradePaxos;
import didatrade.DidaTradePaxosServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class DidaTradeServerState {
    public static final int     DEFAULT_POPULATION = 10;
    int                         max_participants;
    TradeManager                trade_manager;
    ConfigurationScheduler      scheduler;
    int                         base_port;
    int                         my_id;
    RequestHistory              req_history;
    PaxosLog                    paxos_log;

    List<Integer>               all_participants;
    int                         n_participants;
    String[]                    targets;
    ManagedChannel[]            channels;
    DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[] async_stubs;

    private int  current_ballot;

	// Vertical Paxos: highest ballot c known to be complete
	private int completed_ballot = -1;

    private int                 debug_mode;
    private boolean             fastpaxos_on;
	private int                 fast_ballot;
    private int                 fast_next_instance;

	final Object execution_lock = new Object();
	private int                 last_instance = -1;


    MainLoop                    main_loop;
    Thread                      main_loop_worker;

	DebugModeHandler 			debug;

	/*
		[proposer] Multipaxos Phase 1
		Store the last ballot num that this proposer
		did phase 1 so that in the future if we wake up
		we know that we may need to do it again.
	*/
	int 						last_prepared_ballot;


    public DidaTradeServerState(int port, int myself, char schedule, DebugModeHandler debug) {
	this.trade_manager    	  = new TradeManager();
	this.scheduler        	  = new ConfigurationScheduler (schedule);
	this.base_port        	  = port;
	this.my_id            	  = myself;
	this.debug_mode       	  = 0;

	this.fastpaxos_on     	  = false;
	this.fast_ballot          = -1;
	this.fast_next_instance   = 0;

	this.current_ballot   	  = 0;
	this.completed_ballot 	  = -1;
	this.req_history      	  = new RequestHistory();
	this.paxos_log        	  = new PaxosLog();
	this.main_loop        	  = new MainLoop(this);
	this.debug 				  = debug;
	this.last_prepared_ballot = -1;

	// populate manager
	this.trade_manager.populate(DEFAULT_POPULATION);

	// init comms
	this.all_participants = this.scheduler.allparticipants ();
	this.n_participants = all_participants.size();

	this.targets = new String[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) {
	    int target_port = this.base_port + all_participants.get(i);
	    this.targets[i] = new String();
	    this.targets[i] = "localhost:" + target_port;
	    System.out.printf("targets[%d] = %s%n", i, targets[i]);
	}

	this.channels = new ManagedChannel[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.channels[i] = ManagedChannelBuilder.forTarget(this.targets[i]).usePlaintext().build();

	this.async_stubs = new DidaTradePaxosServiceGrpc.DidaTradePaxosServiceStub[this.n_participants];
	for (int i = 0; i < this.n_participants; i++) 
	    this.async_stubs[i] = DidaTradePaxosServiceGrpc.newStub(this.channels[i]);	

	// start worker
	this.main_loop_worker = new Thread (main_loop);
	this.main_loop_worker.start();
    }

    public synchronized int getCurrentBallot() {
		return this.current_ballot;
    }
    
    public synchronized void setCurrentBallot (int ballot) {
		if (ballot > this.current_ballot) {
	    	this.current_ballot = ballot;
		}
    }

    public synchronized void setLastPreparedBallot(int ballot) {
		if (ballot > this.last_prepared_ballot) {
	    	this.last_prepared_ballot = ballot;
		}
    }

    public synchronized int getLastPreparedBallot() {
		return this.last_prepared_ballot;
    }

    
    public int fastQuorum(int ballot) {
		int n_acceptors = this.scheduler.acceptors(ballot).size();
		return (3 * n_acceptors + 3) / 4;
    }

    public synchronized int getCompletedBallot() {
		return this.completed_ballot;
    }

	public synchronized void setCompletedBallot(int ballot) {
		if (ballot > this.completed_ballot) {
			this.completed_ballot = ballot;
		}
		this.notifyAll();
    }

    public synchronized int waitForCompletedBallot(int ballot) {
        while (this.completed_ballot < ballot) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
		return this.completed_ballot;
    }


	public synchronized boolean inFastMode() {
    	return this.fast_ballot == this.current_ballot;
	}

	public synchronized int getFastBallot () {
	return this.fast_ballot;
    }

    public synchronized void setFastBallot (int ballot) {
	this.fast_ballot = ballot;
    }

    public synchronized int getFastNextInstance () {
	return this.fast_next_instance;
    }

    public synchronized void setFastNextInstance (int instance) {
	this.fast_next_instance = instance;
    }

	public synchronized int nextFastInstance() {
		return this.fast_next_instance++;
    }

	public synchronized int getLastInstance() {
	return this.last_instance;
    }

    public synchronized void setLastInstance(int instance) {
	this.last_instance = instance;
	this.notifyAll();
    }

    public synchronized void waitForInstance(int instance) {
	while (this.last_instance < instance) {
	    try { wait(); } catch (InterruptedException e) {}
	}
    }

    public synchronized int getDebugMode () {
	return this.debug_mode;
    }

    public synchronized void setDebugMode(int mode) {
		this.debug_mode = mode;
    }

	public DebugModeHandler getDebugHandler() {
		return this.debug;
	}

	// decoupling purposes
	public void doDebug() {
		this.debug.doDebugMode(this.debug_mode);
	}

}
