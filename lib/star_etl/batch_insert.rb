module StarEtl
  class BatchInsert < Base
    
    attr_accessor :table, :cols, :queue
    
    def initialize(table, cols, fact)
      @table = table
      @cols  = cols
      @queue = []
      @fact  = fact
      @mutex = @fact.mutex
    end
    
    def dump!(force=false)
      if force || @queue.uniq.size >= 200
        @mutex.synchronize {
          @fact.threads.each {|t| t[:wait] = true if t.alive? }
          # puts "stopped threads"
          
          qq     = @queue
          @queue = []
          puts "dumping batch into #{@table} with #{qq.uniq.size} rows"
          begin
            r = sql(%Q{INSERT INTO #{@table} (#{@cols}) VALUES #{qq.compact.uniq.join(',')} })
          rescue
            #raise e unless e.include?("duplicate key value")
            
            qq.compact.uniq.each do |val|
              begin
                r = sql(%Q{INSERT INTO #{@table} (#{@cols}) VALUES #{val} })
              rescue
                puts "did't insert #{val} into #{@table}, it was rejected"
              end
            end
            
          end
          
          # puts "starting threads"
          
          
          @fact.threads.each {|t| t[:wait] = false if t.alive? }
        }
      end

    end
    
    def <<(value)
      if value
        @queue << value
        dump!
      end
    end
    
  end  
end