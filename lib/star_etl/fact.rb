module StarEtl
  class Fact < Base

    attr_accessor :source, :measure, :dest, :dimensions
    attr_reader :threads, :mutex
  
    def initialize
      @dimensions    = []
      @last_id       = 0
      @threads       = []
      @thread_max    = Extractor.options[:workers]
      @mutex         = Mutex.new
      @ready_to_stop = false
      @batches       = {}
    end
  
    def dimension(&block)
      @dimensions << block
    end
    
    def run!
      total = sql(%Q{SELECT count(*) from #{source}}).first["count"]
      puts "extracting from #{total} total records"
      total_chunks = (total.to_i / 200) + 1
      puts "will be done in #{total_chunks} chunks"
      completed = 0

      record = sql(%Q{SELECT * from #{source} limit 1}).first
      insert = {}
      dimensions.each do |dim_block|
        d = Dimension.new(record)
        dim_block.call(d)
        batches[d.name] = BatchInsert.new(d.name, d.columns, self)
        insert["fk_#{d.name}"] = d.pk
      end
      insert[measure]
      batches[source] = BatchInsert.new(source, insert.keys.join(", "), self)
      
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
                insert["fk_#{d.name}"] = d.pk
                ivs = d.insert_values
                batches[d.name] << ivs if ivs
              end  
              insert[measure] = record[measure]
              # insert_record(dest, insert)
              batches[source] << prepare_values(insert.values)
              # STDOUT.print(".") && STDOUT.flush
            end
          
            @mutex.synchronize {
              completed += 1
              puts "#{completed}/#{total_chunks} Completed"
            }
            
          }
      
        end      
      end
      #wait for all threads to finish
      @threads.each {|t| t.join }
      batches.values.each {|b| b.dump!(true) }
    end
    
    private
    
    def get_batch
      @mutex.synchronize {
        records = sql(%Q{SELECT * from #{source} WHERE pk_id > #{@last_id} order by pk_id ASC limit 200})
        @ready_to_stop = records.size < 200
        @last_id = records.last["pk_id"].to_i unless records.empty?
        # puts "last id is now #{@last_id}"
        records
      }
    end
    
    def active_threads
      # @threads.map(&:alive?).delete_if {|t| !t }
      @threads = @threads.select {|t| t.alive? }
    end
    
    def batches
      @mutex.synchronize{
        @batches        
      }
    end
    
  end
end