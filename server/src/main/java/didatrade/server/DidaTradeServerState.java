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

    private ArrayList<Integer>  completed_ballot;

    private int                 debug_mode;
    private boolean             fastpaxos_on;

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
	this.current_ballot   	  = 0;
	this.completed_ballot 	  = new ArrayList<>();
	this.req_history      	  = new RequestHistory();
	this.paxos_log        	  = new PaxosLog();
	this.main_loop        	  = new MainLoop(this);
	this.debug 				  = debug;
	this.last_prepared_ballot = 0;

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

    public synchronized int getCompletedBallot (int instance) {
		return this.completed_ballot.get(instance);
    }

    public int findMaxDecidedBallot () {
	int ballot = -1;
	int length = this.paxos_log.length();

	for (int i=0; i< length; i++) {
	    PaxosInstance entry = this.paxos_log.getEntry(i);
	    if (entry == null)
		return ballot;
	    if (!entry.decided)
		return ballot;
	    else if (entry.accept_ballot > ballot)
		ballot = entry.accept_ballot;

	}
	return ballot;
    }
    
    public synchronized void updateCompletedBallot (int ballot, int instance) {
	// WARNING: THIS ONLY WORKS FOR CONFIGURATIONS WHERE THERE IS NO NEED FOR STATE-TRANSFER!!!!!
	// NEEDS TO BE UPDATE FOR THE PROJECT

		ballot = this.findMaxDecidedBallot ();
		if (ballot > this.completed_ballot.get(instance)) {
			this.completed_ballot.add(instance, ballot);
		}
		this.notifyAll();
    }

	public synchronized void setCompletedBallot (int ballot, int instance) {
		while (this.completed_ballot.size() <= instance) {
			this.completed_ballot.add(-1);
		}

		this.completed_ballot.set(instance, ballot);
		this.notifyAll();
    }

    public synchronized int waitForCompletedBallot(int ballot, int instance) {
        while (this.completed_ballot.get(instance) < ballot) {
            try {
		wait ();
	    }
	    catch (InterruptedException e) {
	    }
	}
		return this.completed_ballot.get(instance);
    }

    public synchronized boolean getFastPaxosMode () {
	return this.fastpaxos_on;
    }

    public synchronized void setFastPaxosMode (boolean mode) {
	this.fastpaxos_on = mode;
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
