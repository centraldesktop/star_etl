module StarEtl
  class FactSource < Base

    attr_accessor :source, :measures, :dest, :dimensions, :time_dimension, :ignore_zero
    attr_reader :threads, :mutex
  
    def initialize
      @dimensions     = []
      @time_dimension = false
      @last_id        = Extractor.options[:start_id]
      @threads        = []
      @thread_max     = Extractor.options[:workers]
      @batch_size     = Extractor.options[:batch_size]
      @mutex          = Mutex.new
      @ready_to_stop  = false
      @batches        = {}
      @ignore_zero    = false
    end
  
    def dimension(&block)
      @dimensions << block
    end
    
    def run!
      total = sql(%Q{SELECT count(*) from #{source}}).first["count"]
      puts "extracting from #{total} total records"
      total_chunks = total.to_i / @batch_size
      puts "will be done in #{total_chunks} chunks"
      completed = 0

      record = sql(%Q{SELECT * from #{source} limit 1}).first
      dimensions.each do |dim_block|
        d = Dimension.new(record)
        dim_block.call(d)
        batches[d.name] = BatchInsert.new(d.name, d.columns)
      end
      Thread.abort_on_exception = true
      
      # puts batches.inspect
      until @ready_to_stop
        until active_threads.size == @thread_max
        
          @threads << Thread.new {
            get_batch.each do |record|
              insert = {}
              dimensions.each do |dim_block|
                d = Dimension.new(record)
                dim_block.call(d)
                insert["fk_#{d.name}"] = d.pk_id
                batches[d.name] << d.insert_record
              end  
              insert["fk_time_dimension"] = round_down_to_minute(record[@time_dimension].to_i) if @time_dimension
              
              
              measures.each_pair do |col, dest|
                i = {col => record[col]}.merge(insert)                
                begin
                  insert_record(dest, i) unless i[col].nil? || (@ignore_zero && [col] == 0)
                rescue ActiveRecord::StatementInvalid => e
                  puts e
                  puts record.inspect
                end
              end

              # STDOUT.print(".") && STDOUT.flush
            end
          
            @mutex.synchronize {
              completed += 1
              puts "#{completed}/#{total_chunks} Completed" if completed.remainder(10) == 0
            }
            
          }
      
        end      
      end
      #wait for all threads to finish
      @threads.map(&:join)
      batches.values.each {|b| b.dump!(true) }
      
      #make sure everything gets inserted
      batches.values.each {|b| b.threads.map(&:join) }
    end
    
    private
    
    def get_batch
      @mutex.synchronize {
        records = sql(%Q{SELECT * from #{source} WHERE pk_id > #{@last_id} order by pk_id ASC limit #{@batch_size}})
        @ready_to_stop = records.size < @batch_size
        @last_id = records.last["pk_id"].to_i unless records.empty?
        # puts "last id is now #{@last_id}"
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
    
  end
end