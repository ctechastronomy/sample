Program does anomoly detection using ring buffers (as queue's for users) and dynamic statistical mods (for quick updates).

Program also allows for snapshotting system state (as a summary of prior processing).

Program might take 6-10 secons to process 600 entries. With snapshotting, that work is only done once.

Program also allows for intra-processing snapshotting (running snapshotting every x input events).  This is useful for when you want to be able to kill the processing, and then restart it (without reprocessing successful processing) and picking up remaining unprocessed items.

Program also allows for sub-second processing (processing events without unique identifiers).  This was necessary because the events have no unique id; and several events come in with the same timestamp.

Program also allows for stream appending to batch processing (for snapshot consistency).  This allows you to run the program multiple times, and it will automatically create a snapshot log for processed stream entries (consistent with snapshot.obj)

Program is organized as a glue program "program.rb" and a SystemState manager.  "program.rb" is intended to glue buffering/file code while SystemState.rb manages the system state.

Extensive error-checking is applied.

No tests have been supplied for the classes; there wasn't enough time to put together a testing library; I have a fair amount of checks in place

Ruby is not my first choice; it is however, the fastest (for functional prototyping)

C++ (minimal-ist) is probably what I would rewrite this in for a production environment.

I would also make this thread-safe, and probably socketed with a registry for sharding users, and objects.  I would also work on vectorizing for hardware-specific; and some directish memory-stuffing.

Graph FOAF is preliminary; there is a better structure for it.

Overall given the short time frame, and the "free-form" nature of the specification, this should be acceptable.

License:  			No license is granted for any purpose, unless Omar Desoky authorizes it in writing.
Copyright:  		@copyright 2017.  Omar Desoky
Patent/License:  	Algo's are patent-pending.  Contact author for use.

Note:

Snapshotting is quite handy; takes a 20 second "init process", and once run, turns it into a 0.997 second marshal.

I put the defaults at the top of program.rb:

  #config params
  WINDOW_SIZE_DEFAULT = 10
  NETWORK_DEPTH_DEFAULT = 2
  
This is what you want to play with.

Documentation:  basically a bunch of comments.  Yes I know, rdoc (and all the pretty modern code annotations for auto-doc generation).  Not enough time.

Ignore the spelling errors; its late.

Hmmm.

Lastly, there are two batch files.

run_tests (deletes any snapshot files, forcing full init/stream processing)

and if you want to see the snapshotting effect, run_tests_with_snapshot after running run_tests.  this will use the snapshot/marshalled object and zip it along.

If you want a human-readable version, turn on the prettyprint option.  this will dump out snapshot.obj.tmp.   It takes a bit to write out.  

I had to make some calls on what the intent was on specific aspects.  I made the best calls I could.  Sometimes that meant storing some extra information (such as user-level transactions).

Note - the method for detection you've chosen is not ideal; it has many flaws - some of which are detailed in the source code.

To anyone interested in a consulting arrangement, I'm available.  odesoky at gmail --  put "CONSULTING" in the subject line.

To anyone with an interesting problem (not just coding), feel free to drop me a line.

Later.





