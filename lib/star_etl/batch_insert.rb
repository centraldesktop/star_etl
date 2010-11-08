module StarEtl
  class BatchInsert < Base
    
    attr_accessor :table, :cols, :queue
    
    def initialize(table, cols)
      @table = table
      @cols  = cols
      @queue = []
      # @mute  = Mutex.new
    end
    
    def dump!(force=false)
      # @mute.synchronize {
        if force || @queue.size > 200
          qq     = @queue
          @queue = []
          puts "dumping batch into #{@table} with #{qq.uniq.size} rows"
          r = sql(%Q{INSERT INTO #{@table} (#{@cols}) VALUES #{qq.uniq.join(',')} })
          puts r.inspect
        end
      # }
    end
    
    def <<(value)
      @queue << value if value
      dump!
    end
    
  end  
end