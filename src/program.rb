require 'json'
require 'optparse'
require 'logger'
require 'date'
require 'pp'

require './src/SystemState.rb'

# @copyright 2017 Omar Desoky.  All rights reserved.
# License:  no licenses are currently issued for any use absent written permission from Omar Desoky.
# Algorithms are patent-pending.  Contact author for licensing/use of patentable algorithmic development


module OutlierDetection
  
  system("clear"); system("cls") #clear terminal (OS-dependent)

  #program defaults
  #input
  INIT_FILENAME   = 'log_input/batch_log.json'
  STREAM_FILENAME = 'log_input/stream_log.json'
  #output
  ANOMOLY_FILENAME  = 'log_output/flagged_purchases.json'
  SNAPSHOT_FILENAME = 'log_output/snapshot.obj'
  SNAPSHOT_AUTOSAVE_EVERY_X_RECORDS = 100000
  #log
  LOG_FILENAME = 'log_output/anomoly_detection.log'
  #config params
  WINDOW_SIZE_DEFAULT = 10
  NETWORK_DEPTH_DEFAULT = 2

  
  options = Hash.new{}  #options hash, to be: read in from env, passed in command line or pipe, embedded in file or stream
  
  #file/stream params
  options[:init_file] = nil 
  options[:stream_file] = nil 
  options[:anomoly_file] = nil 
  #logger options
  options[:log_level] = nil
  options[:log_file] = nil 
  #algo params
  options[:window_size] = nil 
  options[:network_depth] = nil
  options[:override_init_file_params_with_current_params] = true  #ignore init_file params
  #set currently to always use snapshots
  options[:use_snapshot] = true    #have it setup to use the snapshot
  options[:snapshot_file] = nil    
  options[:autosave_every_x_records] = -1   #-1 to skip, set to nil to use default
  options[:snapshot_stream_4append] = false   #copy out stream as processed and snapshotted
  #debug pretty_print snapshot object
  options[:prettyprint_snapshot] = false   #set to true to pretty print snpshot
  
  
  
  #configure inputs
  init_file     = options[:init_file]    || INIT_FILENAME
  stream_file   = options[:stream_file]  || STREAM_FILENAME
  stream_tmp_file = stream_file + ".4append" if options[:snapshot_stream_4append]

  #configure outputs
  anomoly_file  = options[:anomoly_file] || ANOMOLY_FILENAME

  #configure snapshot marshal file
  snapshot_file = options[:snapshot_file] || SNAPSHOT_FILENAME
  snapshot_file_pp = snapshot_file + ".tmp"
  autosave_every_x_records = options[:autosave_every_x_records] || SNAPSHOT_AUTOSAVE_EVERY_X_RECORDS
  
  #configure logger
  log_file      = options[:log_file]     || STDOUT # may wish to use LOG_FILENAME as default
  log_level     = options[:log_level]    || Logger::INFO
  
  #configure algo
  window_size   = options[:window_size] || WINDOW_SIZE_DEFAULT
  network_depth = options[:network_depth] || NETWORK_DEPTH_DEFAULT
  
  #create logger
  logger = Logger.new(log_file, 10, 1024000) #10 oldest logfiles, no bigger than 1 meg
  logger.level = log_level
  
  #configuring base algo. params
  current_window_size = options[:window_size] || WINDOW_SIZE_DEFAULT
  current_network_depth = options[:network_depth] || NETWORK_DEPTH_DEFAULT
  
  #starting program
  last_time = start_time = Time.now
  logger.info('anomoly'){"Beginning anomoly detection with base config: window_size:#{current_window_size}; network_depth:#{current_network_depth}"}

  #validate params
  begin
    SystemState.verify_window_size(current_window_size)
    SystemState.verify_network_depth(current_network_depth)
  rescue OutlierDetectionError  => e
    logger.error('anomoly'){"problem with current parameters: #{e}"}; raise e
  end

  #initialize state
  system_state = nil
  
  # UNMARSHALLING
  
  puts "System Init Time: #{Time.now - last_time}"; last_time = Time.now

  #read in snapshot (unmarshal)
  if options.has_key?(:use_snapshot) && options[:use_snapshot]
    begin
      #note:window_size, network_depth may be different from initialization of current_ versions
      system_state = SystemState.unmarshal(snapshot_file)
      
      #hack: change window_size && change network_depth not implemented; thus base params have to match
      if options[:override_init_file_params_with_current_params]
        if system_state.network_depth != current_network_depth
          logger.error('anomoly.snapshot'){"marshalled structure base-param network_depth doesn't match current_network_depth; silently skipping && reprocessing"}
          raise OutlierDetectionError.new('mismatch in network_depth; implement dynamic changing of network_depth if this feature is desired')
        end
        if system_state.window_size != current_window_size
          logger.error('anomoly.snapshot'){"marshalled structure base-param window_size doesn't match current_window_size; silently skipping && reprocessing"}
          raise OutlierDetectionError.new('mismatch in window_size; implement dynamic changing of window_size if this feature is desired')
        end
      end
      logger.info('anomoly.snapshot'){"Snapshot (#{system_state.version_no}) successfully unmarshalled with last timestamp: #{system_state.last_transaction}"}
    rescue OutlierDetectionError  => e
      #recover if there is a problem with unmarshalling by ignoring snapshot
      logger.error('anomoly.snapshot'){"snapshot unmarshalling unavailable: #{e}"}
      system_state = nil
    end
    
  else
    logger.warn('anomoly.snapshot'){"Snapshot unmarshalling is disabled; this will force reread of init file"}    
  end 
  
  # IIIIIIIIIIIIIIIIINNNNNNIIIIT
  
  logger.info('anomoly.init'){"Beginning read of init file #{init_file}"}
  system_state = SystemState.new() if system_state.nil? #snapshot unavailable; initialize with no specific window _size; no specific network_depth

  puts "Marshalling Time: #{Time.now - last_time}"; last_time = Time.now
 
  #read in batch process (skip anything older than unmarshal)
  if (File.file? init_file)
    #consider:  chunk-reading in json, with marker, similar to socket.io (allow for multi-line json)
    #consider:  chunking similar transactions for batch-processing rather than sax-style line-parsing (allows for pre-queue prep)
    
    File.foreach(init_file).with_index do |line, line_num|
      case line_num
      when 0..0  #n-line header info
        begin
          #simple config, no need to move into SystemState
          init_config = JSON.parse(line)
          init_window_size   = Integer(init_config["T"])
          init_network_depth = Integer(init_config["D"])
          SystemState.verify_window_size(init_window_size)
          SystemState.verify_network_depth(init_network_depth)
          if options[:override_init_file_params_with_current_params]
            #because it doesn't matter (what the grouping was in the batch processing, just ignore it and set to previously verified current_ params)
            #otherwise we'll have to implement changing system settings (why bother for this short demo program?)
            init_window_size   = current_window_size
            init_network_depth = current_network_depth
          end
        rescue OutlierDetectionError  => e
          logger.error('anomoly.init'){"problem with initfile parameters: #{e}"}; raise e
        end
        #change system state to match init parameters
        system_state.change_window_size(init_window_size)
        system_state.change_network_depth(init_network_depth)
      else  #actual events processing
        #note if snapshot file was used/loaded, this will:
        #always skip any event timestamped older than systemstate timestamp
        #always process any events timestamped newer than systemstate timestamp
        #process events timestamped after marker line is encountered (useful only for logs, or append logs)
        
        begin
          #some ghetto prepocessing to speed things up (why Json.parse if you don't need to?)  may want to add in begin..rescue for malformed date-times (with silent skip)
          line_match = line.match(/"timestamp":"(.*?)"/)
          if line_match
            line_timestamp = DateTime.strptime(line_match[1], "%Y-%m-%d %H:%M:%S")
          else
            line_timestamp = nil
          end
          if line_timestamp.nil? || system_state.last_timestamp.nil? || line_timestamp >= system_state.last_timestamp
          #end ghetto processing
          
            #regular style processing
            processed_line_flag = system_state.parse_line(line, line_num) # still takes too long to skip timestamp stuff thats before whats already been processed

            if processed_line_flag
              unless system_state.anomolies_detected.empty?
                anomoly = system_state.anomolies_detected.pop
                #logger.warn('anomoly.init'){anomoly[0]}
              end
            end
            
            #snapshot every mod 1000 events processed (only if there was a change)
            if autosave_every_x_records > 0
              if processed_line_flag
                if system_state.num_processed_lines % autosave_every_x_records == 0
                  logger.info('anomoly.init'){"auto-saving snapshot - #{line_num} (+#{system_state.num_processed_lines})"}
                  system_state.snapshot(snapshot_file, true) #use temp-file
                end
             end
           end
           
          end #ghetto bypass (to speed things up)    
       rescue OutlierDetectionError  => e
          #no op - just skip line
         logger.error('anomoly.init'){"problem parsing line_no #{line_num} in file #{init_file} : #{e}"}         
       end
      end
    end
  else
    logger.error('anomoly.init'){"#{init_file} does not exists!  Nothing to initialize with!"}
  end 
  
  #marshal snapshot (always create, even if user doesn't want to use)
  system_state.snapshot(snapshot_file)
  
  # STREAMING
  
  #todo:  may wish to implement move of "processed" (snapshotted) entries onto batch.json (during stream snapshotting)
    
  puts "Init-Processing Time: #{Time.now - last_time}"; last_time = Time.now
  
  #change system state to match init parameters
  #hack - init file is read in with current_window_size and current_network_depth; should never be different!
  #if required by customer, disable over-rides and implement user_group, user_transactions changes
  system_state.change_window_size(current_window_size)
  system_state.change_network_depth(current_network_depth)
  
  #read in stream && output append-only notifications
  if (File.file? stream_file)
    
    #open notifications for write to file
    logger.warn('anomoly'){"#{anomoly_file} exists; overwriting file"} if (File.file? anomoly_file)
    anomoly_filehandle = File.open(anomoly_file, "w")
    
    #open stream snapshot file for storing newly processed lines
    stream_tmp_file = File.open(stream_tmp_file, "w") if options[:snapshot_stream_4append]
    
    File.foreach(stream_file).with_index do |line, line_num|
    begin
          #some ghetto prepocessing to speed things up (why Json.parse if you don't need to?)  may want to add in begin..rescue for malformed date-times (with silent skip)
          line_match = line.match(/"timestamp":"(.*?)"/)
          if line_match
            line_timestamp = DateTime.strptime(line_match[1], "%Y-%m-%d %H:%M:%S")
          else
            line_timestamp = nil
          end
          if line_timestamp.nil? || system_state.last_timestamp.nil? || line_timestamp >= system_state.last_timestamp
          #end ghetto processing


            #regular style processing
            processed_line_flag = system_state.parse_line(line, line_num, false) #don't force process (because you might re-run same stream file); set to true only for gauranteed one-time buffers
          
            if processed_line_flag
              unless system_state.anomolies_detected.empty?
                anomoly = system_state.anomolies_detected.pop
                logger.warn('anomoly.init'){anomoly[0]}
                anomoly_desc = anomoly[1] 
                anomoly_filehandle.write(%Q({"event_type":"purchase", "timestamp":"#{(anomoly_desc[1]).strftime("%Y-%m-%d %H:%M:%S")}", "id": "#{anomoly_desc[0]}", "amount": "#{'%.02f' % anomoly_desc[2]}", "mean": "#{'%.02f' % anomoly_desc[3]}", "sd": "#{'%.02f' % anomoly_desc[4]}"}\n))
              end
            end
                      
            #snapshot every mod 1000 events processed (only if there was a change)
            if autosave_every_x_records > 0
              if processed_line_flag
                
                # save process lines into file
                stream_tmp_file.write(line) if options[:snapshot_stream_4append] 
                
                if system_state.num_processed_lines % autosave_every_x_records == 0
                  logger.info('anomoly.init'){"auto-saving snapshot - #{line_num} (+#{system_state.num_processed_lines})"}

                  #take snapsho
                  system_state.snapshot(snapshot_file, false) # dont use temp-file (slows things down)
                  
                  #close off new stream snapshots 4append
                  if options[:snapshot_stream_4append]
                    #save 4append
                    stream_tmp_file.close
                    #pseudo:code if you want, take stream_tmp_file and append onto batch_log.json (won't be processed again in stream anyway)
                    stream_tmp_file = File.open(stream_tmp_file, "w")
                  end
                end
             end
           end
        
        end        

        rescue OutlierDetectionError  => e
           #no op - just skip line
           logger.error('anomoly.stream'){"problem parsing line_no #{line_num} in file #{stream_file} : #{e}"}
        end
    
    end

    anomoly_filehandle.close() #close anomoly output
  else
    logger.error('anomoly.stream'){"#{stream_file} does not exists!  Nothing to process!"}
  end 

  puts "Streaming Processing Time: #{Time.now - last_time}"; last_time = Time.now

  puts "** Total Eplased Time Time**: #{Time.now - start_time}"

  # FINISHED
  
  #marshal snapshot (always create, even if user doesn't want to use)
  system_state.snapshot(snapshot_file)  

  #close off new stream snapshots 4append
  if options[:snapshot_stream_4append]
    stream_tmp_file.close
    #pseudo:code if you want, take stream_tmp_file and append onto batch_log.json
    #delete stream_tmp_file and touch empty stream_file after appending
  end
  
  puts "Final Snapshot Save Time: #{Time.now - last_time}"; last_time = Time.now

  if (options[:prettyprint_snapshot])
    #object inspect
    File.open(snapshot_file_pp, 'w') {|f| f.write( system_state.pretty_inspect) }
    puts "PrettyInspectWrite Time: #{Time.now - last_time}"; last_time = Time.now
  end

  logger.close # close logger


end
