module StarEtl
  class BatchInsert < Base
    
    attr_accessor :table, :cols, :queue
    attr_reader   :threads
    
    def initialize(table, size=500)
      @table   = table
      @queue   = []
      @size    = size
      @threads = []
    end
    
    def dump!(force=false)
      if force || @queue.size >= @size

        @insert_q = @queue
        @queue = []
        
        @threads << Thread.new {
          inserted = 0
          @insert_q.uniq.each do |record|
            begin
              insert_record(@table, record)
              inserted += 1
            rescue
            end
          end
          debug "Inserted #{inserted} rows into #{@table}"
        }
        
      end
    end
    
    def <<(value)      
      if value
        @queue << value
      end
      dump!
    end
    
  end  
end