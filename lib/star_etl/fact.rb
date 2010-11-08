module StarEtl
  class Fact < Base

    attr_accessor :source, :measure, :dest, :dimensions
  
    def initialize
      @dimensions    = []
      @last_id       = 0
      @threads       = []
      @thread_max    = 100
      @semaphore     = Mutex.new
      @ready_to_stop = false
      @batches       = {}
    end
  
    def dimension(&block)
      @dimensions << block
    end
    
    def run!
      total = sql(%Q{SELECT count(*) from #{source}}).first["count"]
      puts "extracting from #{total} total records"
      puts "will be done in #{total.to_i / 500} chunks"

      dimensions.each do |dim_block|
        record = sql(%Q{SELECT * from #{source} limit 1}).first
        d = Dimension.new(record)
        dim_block.call(d)
        batches[d.name] = BatchInsert.new(d.name, d.columns)
      end
      
      # puts batches.inspect

      until @ready_to_stop || active_threads.size == @thread_max
        
        @threads << Thread.new {
          records = get_batch
          
          records.each do |record|
            insert = {}
            dimensions.each do |dim_block|
              d = Dimension.new(record)
              dim_block.call(d)
              insert["fk_#{d.name}"] = d.pk
              batches[d.name] << d.insert_values
            end  
            insert[measure] = record[measure]
            insert_record(dest, insert)
            STDOUT.print(".") && STDOUT.flush
          end
          
          batches.values.each {|b| b.dump!(true) }
          
        }
      
      end      
    

      #wait while they work
      until active_threads.empty?
        sleep(1)
      end
          
    end
    
    private
    
    def get_batch
      @semaphore.synchronize {
        records = sql(%Q{SELECT * from #{source} WHERE pk_id > #{@last_id} order by pk_id ASC limit 500})
        @ready_to_stop = records.size < 500
        @last_id = records.last["pk_id"].to_i
        puts "last id is now #{@last_id}"
        records
      }
    end
    
    def active_threads
      @threads.map(&:alive?).delete_if {|t| !t }
    end
    
    def batches
      @semaphore.synchronize{
        @batches        
      }
    end
    
  end
end