require 'date'

# @copyright 2017 Omar Desoky.  All rights reserved.
# License:  no licenses are currently issued for any use absent written permission from Omar Desoky.
# Algorithms are patent-pending.  Contact author for licensing/use of patentable algorithmic development

module OutlierDetection

class OutlierDetectionError < StandardError
  def initialize(msg="My default message")
    super
  end
end

class OutlierAnomoly < StandardError
  attr_reader :msg, :event_descriptor
  def initialize(msg="My default message", event_descriptor=nil)
    @event_descriptor = event_descriptor
    super(msg)
  end
end

class UserDirectory

  def initialize()
    @users = Hash.new
  end
  
  #check if someone is a new user
  def is_user?(uid)
    return(@users.has_key?(uid))
  end

  def is_new_user?(uid)
    return(!(@users.has_key?(uid)))
  end
  
  #creates a new user
  def create_user(uid)
    #only likely to be encountered (directly called) if a purchase is first encounter of a user id
    unless is_user?(uid)
      @users[uid] = Array.new
    else
       raise OutlierDetectionError.new("Can't create a user #{uid} in userdirectory if they already exist!")
    end 
  end
  
  def get_userlist
    return @users.keys
  end
  
  #gets friendship array
  def friends_of(uid)
    return(nil) unless is_user?(uid)
    return(@users[uid])
  end
  
  #adds a friendship (symmetric), returns array of "new users"
  def add_friendship(id1, id2)
    raise OutlierDetectionError.new("add_friendship(#{id1}, #{id2}) called with bad params") if id1.nil? || id2.nil? 
    
    new_users = Array.new
    unless id1 == id2
      unless is_user?(id1)
        create_user(id1)
        new_users.push(id1)
      end
      
      unless is_user?(id2)
        create_user(id2)
        new_users.push(id2)
      end

      #add friendship
      @users[id1].push(id2)
      @users[id2].push(id1)
      return(new_users)
    end
    return(new_users)
    
  end
  
  #removes a friendship
  def remove_friendship(id1,id2)
    raise OutlierDetectionError.new("remove_friendship(#{id1}, #{id2}) called with bad params") if id1.nil? || id2.nil? 

    unless id1 == id2
      #note:  this check only works because we only add friends as we encounter them; 
      #possible to run into situation where delete is requested from one side only (use case issue)
      if is_user?(id1) && is_user?(id2)
        @users[id1] = @users[id1].delete_if{|x| x==id2}
        @users[id2] = @users[id2].delete_if{|x| x==id1}
      else
        raise OutlierDetectionError.new("Can't delete relationships user #{id1}, #{id2} in userdirectory if they dont exist!")
      end
      return(true)
    end
    return(false)
  end
end

class DynaStats
 #can be easily adapted to allow for running statistics on both windowed and infinite (may be interested to see drift)
 attr_reader :current_avg, :current_stdevp
 alias :avg :current_avg
 alias :sd :current_stdevp
 
 def pp
   "mean=#{@current_avg} sd=#{@current_stdevp} sdlvl=#{@sigmalvl} sdlwr=#{@sigmaneg} sdupr=#{@sigmapos}"
 end

 def ppp
   [@current_avg, @current_stdevp]
 end
  
 def initialize(sigma_level=3) 
   @count = 0
   @sum = 0
   @current_avg = 0.0
   @current_stdevp = 0.0
   @square_sum = 0.0
   #hard-coded for speed
   @sigmalvl = Integer(sigma_level) #sigma level is integer only (to allow for easy comparison of memoized version)
   @sigmapos = 0.0
   @sigmaneg = 0.0
 end

 def add_amount(amount_struct, lru_struct)
     lru_amount   = nil  #default to nil
     lru_amount   = lru_struct[1] unless lru_struct.nil?
     curr_amount  = amount_struct[1]
            
      #prelim calculations (only increment count if buffer not full; always remove LRU if it exists)
      @count += 1 if lru_amount.nil?
      @sum   += curr_amount
      @sum   -= lru_amount unless lru_amount.nil?
      @current_avg = (@sum.to_f / @count).round(2)  #ensure either @sum or @count is float (in case you decide to use integers to represent to avoid floating point summation errors)
      #stdev.p
      @square_sum   += curr_amount * curr_amount
      @square_sum   -= lru_amount * lru_amount unless lru_amount.nil?
      n = (1.0 / @count)
      n2 = n * n
      sum1 = n * @square_sum
      sum2 = n2 * ( @sum * @sum)
      #step most likely to result in a run-time error
      begin
        @current_stdevp = (Math.sqrt(sum1 - sum2)).round(2)
      rescue
        raise OutlierDetectionError.new("can't take sqrt of negaive sum #{sum1 - sum2}:\tfrom #{sum1} - #{sum2}")
      end
      #update intervals
      plus_minus = @sigmalvl * @current_stdevp
      @sigmaneg = (@current_avg - plus_minus).round(2)   
      @sigmapos = (@current_avg + plus_minus).round(2)
 end

 def outlier_intervals(level=3)
   if level = @sigmalvl
    sigmaneg = @sigmaneg
    sigmapos = @sigmapos
   else
    sigmaneg = (@current_avg - level * @current_stdevp).round(2)
    sigmapos = (@current_avg + level * @current_stdevp).round(2)
   end
   return[sigmaneg, sigmapos]
 end
   
 def is_outlier?(valcheck, level=3)
   return(false) unless @count > 1
   intervals = outlier_intervals(level)
   sigmaneg = intervals[0]
   sigmapos = intervals[1]
   #puts "#{valcheck} < #{sigmaneg} or #{valcheck} > #{sigmaneg}"
   return((valcheck < sigmaneg) || (valcheck > sigmapos))
end
 
end

class RingBuffer
  attr :windowed_stats, :lifetime_stats, :count
  
  def initialize(size)
    @size = size
    @start = 0
    @count = 0
    @buffer = Array.new(size)
    @windowed_stats = DynaStats.new()  #windowed stats
    #@lifetime_stats = DynaStats.new() #lifetime stats
  end

  def full?
    @count == @size
  end

  def empty?
    @count == 0
  end

  def push(value)
      stop = (@start + @count) % @size

      #get lru to be popped out (before it is overwritten!)
      lru_value = lru_val()
      #dynastat update (structs=[timestamp, amount])
      @windowed_stats.add_amount(value, lru_value) 
      #@lifetime.add_amount(value, nil)
      
      @buffer[stop] = value
      if full?
        @start = (@start + 1) % @size
      else
        @count += 1
      end

      value

  end
  
  def lru_timespamp
    return(nil) unless full?
    return(lru_val[0])
  end

  def lru_val()
    return nil unless full? 
    return @buffer[@start] 
  end
  
  def get_ordered_array()
    temp = @buffer.clone
    temp.rotate(@start)
    return(temp)
  end
  
  def get_buffer()
    return(@buffer)
  end

  def push_and_return_lru(value)
      stop = (@start + @count) % @size

      #get lru to be popped out (before it is overwritten!)
      lru_value = lru_val()
      #dynastat update (structs=[timestamp, amount])
      @windowed_stats.add_amount(value, lru_value) 
      #@lifetime_stats.add_amount(value, nil)

      @buffer[stop] = value
      
      if full?
        @start = (@start + 1) % @size
      else
        @count += 1
      end
            
      return(lru_value)
  end

  alias :<< :push

  def shift
      remove_element
  end

  def flush
    values = []
      while !empty?
        values << remove_element
      end
    values
  end

  def clear
    @buffer = Array.new(@size)
    @start = 0
    @count = 0
  end

  def join(ch)
    return(@buffer.join(ch))
  end

  private

  def remove_element
    return nil if empty?
    value, @buffer[@start] = @buffer[@start], nil
    @start = (@start + 1) % @size
    @count -= 1
    value
  end
end


class UserTransactionHistory

  def initialize(window_size)
    @window_size = window_size
    @gaurd_size = 2 #prefer at least 20 samples
    @target_abnormal_perc = 0.60 #3sd+-
    @user_transactions = Hash.new
    @abnormal_detections = 0
    @total_checked = 0
  end
  
  def get_transactions(uid)
    return(@user_transactions[uid])
  end
  
  def change_window_size(target_window_size)
    raise OutlierDetectionError.new("have to modify something #{@window_size} #{target_window_size}")
  end

  def add_purchase(uid, amount, timestamp)
    if @user_transactions.has_key?(uid)
      lru = @user_transactions[uid].push_and_return_lru([timestamp, amount])
      return(lru)
    else      
      @user_transactions[uid] = RingBuffer.new(@window_size)
      @user_transactions[uid].push_and_return_lru([timestamp, amount])
      return(nil)
    end
  end

  def is_anomolous_purchase_for_user?(uid, amount, event_timestamp) #should take place before transaction is added soas to include current transaction
    #return(is_anomolous_purchase_for_user_with_autoadjust_gaurd?(uid, amount, event_timestamp))
    #return(is_anomolous_purchase_for_user_with_fixed_gaurd?(uid, amount, event_timestamp))
    #return(is_anomolous_purchase_for_user_without_gaurd?(uid, amount, event_timestamp))
    flag = is_anomolous_purchase_for_user_without_gaurd?(uid, amount, event_timestamp)
    if flag
      rb_stats = @user_transactions[uid].windowed_stats
      raise OutlierAnomoly.new("Anomoly detected: user:#{uid} amount:#{amount} time:#{event_timestamp} :: #{rb_stats.pp}",[uid, event_timestamp, amount,rb_stats.ppp].flatten)
      return(flag)
    else
      return(flag)
    end
  end


  #note: 3sd sucks because: a) sample size affects convergence; b) sample window needs to be compensated for sampling procedure c) doesn't handle trending well d) population may not be normal
  #note: simplest solution is dynamically setting a gaurd window on rb.count until the total events reported match the expected ratio for the sd provided
  #note: this is particularly ghetto (afixes the n-sample problem) but won't adjust for diversity of sources or trending; 
  #note: also note collapse of sd as a function of mean with multiple sources will lead to artificially low sd's
  #correct solution is to dynamically adjust sigma-delta AND adjust sigma windows to compensate for mean-sampling error and collapse of sd via covar
  #note there will be a sd reduction in grouping multiple users
  
  def is_anomolous_purchase_for_user_with_autoadjust_gaurd?(uid, amount, event_timestamp) #should take place before transaction is added soas to include current transaction
    # works best by fixing notifications as a percentage of events processed
    # autoadjusts up gaurd until such a time as it acts as a debounce for low-n events.
    # allows for actual convergence on estimated required gaurd-size 
    # expect that grouped sd will actually require significantly less samples
    
    @total_checked += 1  #rough checking statistic

    if @user_transactions.has_key?(uid)
      rb = @user_transactions[uid]
      rb_stats = rb.windowed_stats
         
      #provides abornmal notification as percentage of processed - also note, uses gaurd of min N samples before error detection kicks in
      if rb_stats.is_outlier?(amount) && rb.count > @gaurd_size
        @abnormal_detections += 1
        @percent_abnormal = Integer(@abnormal_detections/ @total_checked.to_f * 10000.0)/100.0
        
        ##dynamically adjust gaurd up to account for small-n pop in SD notifications
        #note not an issue that gaurd balloons up because its ringbuffers (so a high gaurdsize towards the end is in practice, using all information possible)
        @gaurd_size = [@gaurd_size + 1, @window_size].min if @percent_abnormal > @target_abnormal_perc
        
        #puts "WINDOWED\t#{@percent_abnormal}%\t@#{@gaurd_size}\tABNORMAL AMOUNT:#{amount}\t:: for USER #{uid} -- #{rb.count}: #{rb_stats.outlier_intervals.inspect}"
        return true
      end
      return(false)
    end
    return(false)
  end


  def is_anomolous_purchase_for_user_with_fixed_gaurd?(uid, amount, event_timestamp) #should take place before transaction is added soas to include current transaction
    #note a fixed gaurd (requiring 3 or higher) improves convergence somewhat due to statistical swamping
    #... but overall large outlying effect of initial abnormal notification still present
    
    @total_checked += 1  #rough checking statistic

    if @user_transactions.has_key?(uid)
      rb = @user_transactions[uid]
      rb_stats = rb.windowed_stats
         
      #provides abornmal notification as percentage of processed - also note, uses gaurd of min N samples before error detection kicks in
      if rb_stats.is_outlier?(amount) && rb.count > @gaurd_size
        @abnormal_detections += 1
        @percent_abnormal = Integer(@abnormal_detections/ @total_checked.to_f * 10000.0)/100.0
        #puts "WINDOWED\t#{@percent_abnormal}%\t@#{@gaurd_size}\tABNORMAL AMOUNT:#{amount}\t:: for USER #{uid} -- #{rb.count}: #{rb_stats.outlier_intervals.inspect}"
        return true
      end
      return(false)
    end
    return(false)
  end
  
  def is_anomolous_purchase_for_user_without_gaurd?(uid, amount, event_timestamp) #should take place before transaction is added soas to include current transaction
    #note high notification rate swamps abnormality due to high number of users (low-n samples * large m users)
    
    @total_checked += 1  #rough checking statistic

    if @user_transactions.has_key?(uid)
      rb = @user_transactions[uid]
      rb_stats = rb.windowed_stats
         
      #provides abornmal notification as percentage of processed - also note, uses gaurd of min N samples before error detection kicks in
      if rb_stats.is_outlier?(amount) 
        @abnormal_detections += 1
        @percent_abnormal = Integer(@abnormal_detections/ @total_checked.to_f * 10000.0)/100.0
        #puts "WINDOWED\t#{@percent_abnormal}%\t@#{@gaurd_size}\tABNORMAL AMOUNT:#{amount}\t:: for USER #{uid} -- #{rb.count}: #{rb_stats.outlier_intervals.inspect}"
        return true
      end
      return(false)
    end
    return(false)
  end

end

class UserGroups

  def initialize(network_depth, window_size)
    @network_depth = network_depth
    @window_size = window_size
    @groups_counter = 0
    #group_specific stuff
    @groups_buffers = Hash.new
    @groups_users = Hash.new
    @users_groups = Hash.new
  end

   def add_friendship(id1, id2, user_transactions, user_directory)
     unless(id1 == id2) #do nothing if ids are the same
        group_id1 = @users_groups[id1]
        group_id2 = @users_groups[id2]
        
       case
        when group_id1.nil? && group_id2.nil?
          #not part of any group
          new_group_id = create_new_group()
          subscribe_user_to_group(id1, new_group_id, user_transactions)
          subscribe_user_to_group(id2, new_group_id, user_transactions)
          resynch_buffers(new_group_id, user_transactions)
          
        when !(group_id1.nil?) && group_id2.nil?  #user2 is new, user1 isn't
          subscribe_user_to_group(id2, group_id1, user_transactions)
          resynch_buffers(group_id1, user_transactions)
  
        when group_id1.nil? && !(group_id2.nil?)  #user1 is new, user2 isn't
          subscribe_user_to_group(id1, group_id2, user_transactions)
          resynch_buffers(group_id2, user_transactions)
  
        when !(group_id1.nil?) && !(group_id2.nil?)
          unless group_id1 == group_id2  #do nothing if they're in the same group!
            new_group_id = create_new_group()
            
            users_from_group1 = @groups_users[group_id1]
            users_from_group2 = @groups_users[group_id2]
            
            
            users_from_group1.each do |uid|
              move_user_to_group(uid, new_group_id, user_transactions)
            end
           
           users_from_group2.each do |uid|
              move_user_to_group(uid, new_group_id, user_transactions)
            end
            

            
            
            # **** Too error prone! ***
            #subscribe_user_to_group(id1, new_group_id, user_transactions)
            ##subscribe_user_to_group(id2, new_group_id, user_transactions)
            ##delete group
            #users_to_reset  = delete_group(group_id1)
            #users_to_reset.each do |user_id|
            #  subscribe_user_to_group(user_id, new_group_id, user_transactions)
            #end
            ##delete group
            #users_to_reset  = delete_group(group_id2)
            #users_to_reset.each do |user_id|
            #  subscribe_user_to_group(user_id, new_group_id, user_transactions)
            #end
            resynch_buffers(new_group_id, user_transactions)
          end
       end
     end
   end
   
   def delete_group(group_id)
      users_to_reset = @groups_users[group_id]
      @groups_buffers.delete(group_id)
      @groups_users.delete(group_id)
      return(users_to_reset)
   end
      

   def delete_friendship(id1, id2, user_transactions, user_directory)
     unless(id1 == id2) #do nothing if ids are the same
      group_id1 = @users_groups[id1]
      group_id2 = @users_groups[id2]
      if (group_id2 == group_id1) #otherwise why would we need to defriend the group?
        foaf_flag = foaf_path(id1, id2, user_directory, @network_depth) #foaf-check (if there is another link through foaf, then no need to do anything)
        unless foaf_flag
          #user2 moves on
          new_group_id = create_new_group()
          move_user_to_group(id2, new_group_id, user_transactions)
          foaf_network = foaf_cover(id2, user_directory, @network_depth)
          unless foaf_network.empty?
            #puts "moving #{foaf_network}"
            foaf_network.each do |friend_id|
              move_user_to_group(friend_id, new_group_id, user_transactions)
            end
          end
          #todo:  resynch group code
          resynch_buffers(group_id1, user_transactions)
          resynch_buffers(new_group_id, user_transactions)
        end
      end
     end
   end
   
  def resynch_buffers(group_id, user_transactions)
    #resynch group code using replay + group membership
    raise "group: #{group_id} doesn't exist" unless @groups_users.has_key?(group_id)
    groups_users = @groups_users[group_id]
    resynch_buffer = Array.new
    groups_users.each do |user_id|
      #puts "retrieving user transaction history (if it exists)"
      user_transaction_history = user_transactions.get_transactions(user_id)
      unless user_transaction_history.nil?
        transactions = user_transaction_history.get_ordered_array
        transactions.each do |transaction|
          unless transaction.nil?
            timestamp = transaction[0]
            amount = transaction[1]
            uid = user_id
            resynch_buffer.push([timestamp, amount, uid])
           end
        end
      end
    end
    if resynch_buffer.empty?
      @groups_buffers[group_id] = RingBuffer.new(@window_size)
    else
      #todo: just replace this with an array.top (no need to replay entire array)
      @groups_buffers[group_id] = RingBuffer.new(@window_size)
      #puts "resynching buffer: #{resynch_buffer.size} entries"
      resynch_buffer.sort!
      resynch_buffer.each do |buffer_entry|
            timestamp = buffer_entry[0]
            amount = buffer_entry[1]
            uid = buffer_entry[2]
            #puts "replaying #{timestamp} $#{amount} as #{uid} in group #{group_id}" 
            add_purchase(group_id, uid, amount, timestamp)
      end
    end
  end
   
  #covering set for foaf todo: implement cycle checking
  def foaf_cover(uid, user_directory, level)
    friends = user_directory.friends_of(uid)
    if level == 2
      return(friends)
    else
      people_seen = Array.new
      people_seen.push(uid)
      friends.each do |friend_id|
        people_seen.push(friend_id)
        people_seen.push(foaf_cover(friend_id, user_directory, level - 1))
      end
      return people_seen.flatten.uniq
    end
  end    
  
  #depth-first search todo: implement cycle checking
  def foaf_path(id1, id2, user_directory, level=@network_depth, visited_cycle_check={})
    #puts "foaf_path(#{[id1, id2, level, @network_depth, visited_cycle_check].inspect}"
    
    if level == 2
      #check friends
      friends = user_directory.friends_of(id1)
      return(friends.include?(id2))
    else
      return(true) if id1 == id2
      
      #get list of friends
      friends = user_directory.friends_of(id1)
      
        #call foaf_path on each friend with one less level and || it (nevermind cycles for right now)
        flag = false
        friends.each do |friend_id|
        flag = flag || foaf_path(friend_id, id2, user_directory, level - 1, visited_cycle_check)
      end
      return flag
    end
  end
  
 
  
   def create_new_user(uid, user_transactions)
      raise "new user #{uid} can't have pre-existing group!" if @users_groups.has_key?(uid)
      new_group_id = create_new_group()
      subscribe_user_to_group(uid, new_group_id, user_transactions)
      return(new_group_id)
   end
  
   def create_new_group
      @groups_counter += 1
      group_id = @groups_counter
      @groups_buffers[group_id] = RingBuffer.new(@window_size)
      @groups_users[group_id] = Array.new()
      return (group_id)
   end
   

   def subscribe_user_to_group(uid, group_id, user_transactions)
     @groups_users[group_id].push(uid)
     @users_groups[uid] = group_id
   end
   
   def move_user_to_group(uid, new_group_id, user_transactions)
     old_group_id = @users_groups[uid]
     @groups_users[new_group_id].push(uid)
     @groups_users[old_group_id].delete(uid)
     @users_groups[uid] = new_group_id    
   end
   
   def get_users_group(uid)
     return(@users_groups[uid])
   end
   
   def add_purchase(group_id, uid, amount, timestamp)
    raise "group: #{group_id} doesn't exist" unless @groups_buffers.has_key?(group_id)
    if @groups_buffers.has_key?(group_id)
      lru = @groups_buffers[group_id].push_and_return_lru([timestamp, amount, uid])
      return(lru)
    else      
      @groups_buffers[group_id].push_and_return_lru([timestamp, amount, uid])
      return(nil)
    end
   end

  def is_anomolous_purchase_for_group?(group_id, uid, amount, event_timestamp) #should take place before transaction is added soas to include current transaction
    raise "group: #{group_id} as requested by uid: #{uid} doesn't exist" unless @groups_buffers.has_key?(group_id)
    flag = is_anomolous_purchase_for_group_without_gaurd?(group_id, uid, amount, event_timestamp)
    if flag
      rb_stats = @groups_buffers[group_id].windowed_stats
      raise OutlierAnomoly.new("Anomoly detected: group_id: #{group_id} user:#{uid} amount:#{amount} time:#{event_timestamp} :: #{rb_stats.pp}",[uid, event_timestamp, amount,rb_stats.ppp].flatten)
      return(flag)
    else
      return(flag)
    end
  end
  def is_anomolous_purchase_for_group_without_gaurd?(group_id, uid, amount, event_timestamp)
    raise "group: #{group_id} doesn't exist" unless @groups_buffers.has_key?(group_id)
    rb = @groups_buffers[group_id]
    rb_stats = rb.windowed_stats
    if rb_stats.is_outlier?(amount) 
      return true
    end
    return(false)
  end
   
   def change_network_depth(target_network_depth)
     raise OutlierDetectionError.new("have to modify something #{@network_depth} #{target_network_depth} - not implemented") 
   end
   
  def change_window_size(target_window_size)
    raise OutlierDetectionError.new("have to modify something #{@window_size} #{target_window_size}")
  end
end


class SystemState
  MARSHALL_VERSION = "001_00_00"
  def self.get_version
    return(MARSHALL_VERSION)
  end

  attr_reader :version_no           #required for marshalling veersion checks
  attr_reader :last_transaction     #sub-second profiling as events do not have unique signature
  attr_reader :last_timestamp       #made available to ghetto-parse and check timestamps (speed it up)
  attr_reader :num_processed_lines  #for snapshot every x functionality
  
  attr_reader :anomolies_detected # queu for anomolies messages
  
  #for marshalling checks (because change window size && change network depth not implemented)
  attr_reader :window_size
  attr_reader :network_depth

  #requires window_size, network_depth to allow for snapshotting state
  def initialize
    @num_processed_lines = 0
    
    @version_no = MARSHALL_VERSION
    
    #for datastructure initialization
    @window_size = nil
    @network_depth = nil

    #services (can be extended to be parallelizabe, and machine-independent using gc, registry, or d.obj)
    @users = UserDirectory.new()
    
    #window-size specific class
    @users_transactions = nil
    #network-depth specific class
    @user_groups = nil 
    
    # used for skip-parsinging initialization
    @last_timestamp = nil
    @last_transaction_line = nil
    @process_transaction_line = false
    
    #some experimentation
    @process_user_specific_anomolies = false
    
    @anomolies_detected = Array.new
  end
  
  #dynamically change window size
  def change_window_size(target_window_size)
    case
      when @window_size == target_window_size
        #no-op
      when @window_size.nil? || @users_transactions.nil?
        @window_size = target_window_size
        @users_transactions = UserTransactionHistory.new(@window_size) 
      else
        @users_transactions.change_window_size(target_window_size)
        @users_groups.change_window_size(target_window_size)
    end
  end
  
  #dynamically change network_depth
  def change_network_depth(target_network_depth)
    case
      when @network_depth == target_network_depth
        #no-op
      when @network_depth.nil? || @user_groups.nil?
        @network_depth  = target_network_depth
        @user_groups = UserGroups.new(target_network_depth, @window_size)
      else
        @user_groups.change_network_depth(target_network_depth)
    end 
  end
  
  # abstraction allows for chunking of events
  def parse_line(line, line_no, force_processing=false)  
    #puts "parseline(#{line_no})\t#{line}"

    begin
      json_line = JSON.parse(line)
    rescue StandardError => e
      return(false)  #can't parse into json so why bother (could raise error)
    end
    
    #may want to add in begin..rescue for malformed date-times (with silent skip)
    line_timestamp = json_line["timestamp"]
    timestamp = DateTime.strptime(line_timestamp, "%Y-%m-%d %H:%M:%S")
    
    start_processing_on_next_line = false
    
    #initialize sub-second profiling if it hasn't been used before (i.e. blank object, or no snapshot)
    #force processing flag added to allow reuse of same procedure for "stream_log.json"
    if @last_timestamp.nil? || force_processing==true
      #trivially update sub-second stamping to parse in this line and future lines
      @last_timestamp = timestamp
      @last_transaction_line = line 
      @process_transaction_line = true
    end 
    
    #required for basic subsecond profiling (no unique key for events)
    case
    when timestamp < @last_timestamp
      #never read in data older than snapshot/system state 
      @process_transaction_line = false
      return(false) #to get benefit of speed up quick out immediately!
    when timestamp == @last_timestamp
      #when in same second, check if we are cleared to read in (hit marker line)
      if @process_transaction_line
        #update stamps/ilnes
        @last_timestamp = timestamp
        @last_transaction_line = line
      else
        if (line == @last_transaction_line)
          #check if we hit marker line
          start_processing_on_next_line = true
        end        
      end 
    when timestamp >= @last_timestamp
      #always read in data where timestamp is greater than time stamp 
      @last_timestamp = timestamp
      @last_transaction_line = line 
      @process_transaction_line = true
    end
    
    #actually process event (if we are allowed!)
    if(@process_transaction_line)
      parse_event(timestamp, json_line, line, line_no)
      return(true)
    end
    
    #post process marker line (here to prevent double-count)
    if start_processing_on_next_line
          @process_transaction_line = true
     end 

     return(false)
  end
  
  def parse_event(event_timestamp, event_hash, line, line_no)
      line_event_type = event_hash["event_type"]
      @num_processed_lines += 1
      
      #puts "#{@num_processed_lines}::#{line_no}\t#{event_timestamp} #{line_event_type}\t#{event_hash["id"]}\t#{event_hash["id1"]}\t#{event_hash["id2"]}\t#{event_hash["amount"]}"
      #puts "line_no(#{line_no}):#{event_hash}"

      case line_event_type
      when "befriend"
        #extract ids and convert to unitary storage sym
        id1 = event_hash["id1"].to_sym 
        id2 = event_hash["id2"].to_sym
        
        #puts "creating #{id1} #{id2}"
        new_users = @users.add_friendship(id1,id2)
        #puts "***** friending #{id1} #{id2}"
        @user_groups.add_friendship(id1,id2, @users_transactions, @users)
        #puts @user_groups.pretty_inspect

      when "unfriend"
        #extract ids and convert to unitary storage sym
        id1 = event_hash["id1"].to_sym
        id2 = event_hash["id2"].to_sym

        #puts "deleting #{id1} #{id2}"
        @users.remove_friendship(id1,id2)

        #puts "***** UNfriending #{id1} #{id2}"
        @user_groups.delete_friendship(id1,id2, @users_transactions, @users)  #need access to directory to check for FOAF
        #puts @user_groups.pretty_inspect
        
      when "purchase"
        uid = event_hash["id"].to_sym
        amount = Float(event_hash["amount"])
        
        
        uid_newuser = @users.is_new_user?(uid)
        if uid_newuser
          # create the user in the user directory, but don't do anything with groups as they will be picked up iff and only if they actually friend someone
          @users.create_user(uid) 
        end
        
        #get this users group_no
        group_id = @user_groups.get_users_group(uid)
        if group_id.nil? 
          group_id = @user_groups.create_new_user(uid, @users_transactions)
        end  
        
        #check anomouls nature of transactions
        begin
          user_anomoly_detected = @user_groups.is_anomolous_purchase_for_group?(group_id, uid, amount, event_timestamp)
        rescue OutlierAnomoly => e
          @anomolies_detected.push([e.to_s, e.event_descriptor])
        end
  
       unless group_id.nil?
          #post this to the groups ringbuffer
          @user_groups.add_purchase(group_id, uid, amount, event_timestamp)
        end
        
        #puts @user_groups.pretty_inspect
        
        #storing WINDOW_SIZE purchases for each user (to aid in reconstitution of group queues)
        @users_transactions.add_purchase(uid, amount, event_timestamp)
        
      else
        raise OutlierDetectionError.new("Unknown event encountered during line parsing #{line_event_type} at: #{event_timestamp}")
      end
  end
  
  def snapshot(snapshot_filename, use_temp_file = false)  #writeout snapshot
    #note: marshalling does not support Mutex's (must be extended)
    if use_temp_file
      #uses a temp file mechanism in case write is interrupted
      snapshot_file_tmp = snapshot_filename + ".tmp"
      #note:for cross-os binary, cross-ruby-version portability may want to replace with json marshalling/unmarshalling
      File.open(snapshot_file_tmp, "w"){|to_file| Marshal.dump(self, to_file)}
      File.rename snapshot_file_tmp, snapshot_filename
    else
      #write direct to file
      #note: if program is modd'ed to threaded can use atomicwrite
      File.open(snapshot_filename, "w"){|to_file| Marshal.dump(self, to_file)}
    end      
  end  
  
  def reset_process_transaction_line_flag
    @process_transaction_line = false
  end

  def self.unmarshal(snapshot_filename)  #read in snapshot (if possible)
    #note:for cross-os binary, cross-ruby-version portability may want to replace with json marshalling/unmarshalling
    if (File.file? snapshot_filename)
      system = File.open(snapshot_filename, "r"){|from_file| Marshal.load(from_file)}
      system.reset_process_transaction_line_flag
      unless system.version_no == SystemState.get_version
        raise OutlierDetectionError.new("Snapshot #{snapshot_filename} version:#{system.version_no} differs from current class version:#{MARSHALL_VERSION}")
      end
      return (system) 
    else
      raise OutlierDetectionError.new("Snapshot #{snapshot_filename} does not exist for unmarshalling")
    end
  end
  
  #basic param stuff
  def self.verify_window_size(target_window_size)
      raise OutlierDetectionError.new("target_window_size:#{target_window_size} not valid; must be >=1") unless target_window_size >= 2
  end
  def self.verify_network_depth(target_network_depth)
      raise OutlierDetectionError.new("target_network_depth:#{target_network_depth} not valid; must be >=2") unless target_network_depth >= 1
  end

  private

end


end