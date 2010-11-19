module StarEtl
  class FactSource < Base

    attr_accessor :source, :measures, :dest, :dimensions, :time_dimension, :ignore_zero
    attr_reader :threads, :mutex
  
    def initialize
      @dimensions     = []
      @measures       = []
      @time_dimension = false
      @threads        = []
      @thread_max     = Extractor.options[:workers]
      @batch_size     = Extractor.options[:batch_size]
      @mutex          = Mutex.new
      @ready_to_stop  = false
      @batches        = {}
      @nullify_zero   = true
    end
  
    def dimension(&block)
      @dimensions << block
    end
    
    def run!
      get_id_range
      puts %Q{SELECT count(*) from #{source} WHERE #{@id_range.call}}
      total = sql(%Q{SELECT count(*) from #{source} WHERE #{@id_range.call}}).first["count"]
      puts "extracting from #{total} total records from #{source}"
      @total_chunks = total.to_i / @batch_size
      puts "will be done in #{@total_chunks} chunks"
      @completed = 0
      
      init_batches
      
      Thread.abort_on_exception = true
      
      # puts batches.inspect
      until @ready_to_stop
        until @ready_to_stop || active_threads.size == @thread_max
          @threads << spawn_thread
        end
      end

      #wait for all threads to finish
      @threads.map(&:join)
      batches.values.each {|b| b.dump!(true) }
      
      #make sure everything gets inserted
      batches.values.each {|b| b.threads.map(&:join) }
    end
    
    def spawn_thread
      Thread.new {
        get_batch.each do |record|
          insert = {}
          dimensions.each do |dim_block|
            d = Dimension.new(record)
            dim_block.call(d)
            insert["fk_#{d.name}"] = d.pk_id
            batches[d.name] << d.insert_record
          end  
          insert["fk_time_dimension"] = round_down_to_minute(record[@time_dimension].to_i) if @time_dimension
          
          
          measures.dclone.each do |m|
            
            dest = m.delete(:dest)
            
            i = {}
            m.each_pair do |d_col, s_col|
              value = record[s_col]
              i[d_col.to_s] = value unless (@nullify_zero && value == 0 || value == '0')
            end
            
            begin
              unless i.values.empty? || i.values.map(&:nil?).uniq == [true]
                insert_record(dest, i.merge(insert))
              end
            rescue ActiveRecord::StatementInvalid => e
              debug e
              debug record.inspect
            end
          end

          # STDOUT.print(".") && STDOUT.flush
        end
      
        @mutex.synchronize {
          @completed += 1
          puts "#{@completed}/#{@total_chunks} Completed" if @completed.remainder(10) == 0
        }
        
      }
    end
    
    private
    
    def init_batches
      record = sql(%Q{SELECT * from #{source} limit 1}).first
      dimensions.each do |dim_block|
        d = Dimension.new(record)
        dim_block.call(d)
        batches[d.name] = BatchInsert.new(d.name)
      end
    end
    
    def get_batch
      @mutex.synchronize {
        ss = %Q{SELECT * from #{source} WHERE #{@id_range.call} order by datestamp ASC limit #{@batch_size}}
        debug ss
        records = sql(ss)
        @ready_to_stop = records.size < @batch_size
        @last_id = records.last["datestamp"].to_i unless records.empty?
        debug "last id is now #{@last_id}"
        records
      }
    end
    
    def active_threads
      @threads = @threads.select {|t| t.alive? }
    end
    
    def batches
      @mutex.synchronize{
        @batches        
      }
    end
    
    def get_id_range
      get_last_id
      @id_range = lambda {"(datestamp > #{@last_id} AND datestamp < #{@_to_id_})"}
    end
    
    def get_last_id
      info = sql(%Q{SELECT * from etl_info WHERE table_name = '#{source}' })
      @last_id = if info.empty?
        sql(%Q{INSERT INTO etl_info (last_id, table_name) VALUES (0, '#{source}') })
        0
      else
        info.first["last_id"]
      end
      @_to_id_ = sql(%Q{SELECT datestamp FROM #{source} ORDER BY datestamp desc LIMIT 1}).first["datestamp"]
      
      if @last_id && @_to_id_
        sql(%Q{UPDATE etl_info SET last_id = #{@_to_id_} WHERE table_name = '#{source}'})
      end
    end
    
  end
end