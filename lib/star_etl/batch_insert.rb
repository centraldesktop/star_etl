module StarEtl
  class BatchInsert < Base
    
    attr_accessor :table, :cols, :queue
    
    def initialize(table, cols, fact, size=200)
      @table = table
      @cols  = cols
      @queue = []
      @fact  = fact
      @mutex = @fact.mutex
      @size  = size
    end
    
    def dump!(force=false)
      if force || @queue.uniq.size >= @size
        @mutex.synchronize {
          @fact.threads.each {|t| t[:wait] = true if t.alive? }
          # puts "stopped threads"
          
          qq     = @queue.compact.uniq
          @queue = []
          puts "dumping batch into #{@table} with #{qq.size} rows"
          begin
            r = sql(%Q{INSERT INTO #{@table} (#{@cols}) VALUES #{qq.join(',')} })
          rescue
            #raise e unless e.include?("duplicate key value")
            qq.each do |val|
              begin
                r = sql(%Q{INSERT INTO #{@table} (#{@cols}) VALUES #{val} })
              rescue => e
                puts e
                puts "did't insert #{val} into #{@table}, it was rejected"
              end
            end
            
          end
          
          # puts "starting threads"
          
          sleep(1)
          
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