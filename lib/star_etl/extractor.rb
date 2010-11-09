module StarEtl  
  class Extractor
    
    class << self
      def connect!(db_config)
        @semaphore = Mutex.new
        
        ActiveRecord::Base.establish_connection(db_config)
        @conn = ActiveRecord::Base.connection
      end
    
      def connection
        @semaphore.synchronize { @conn }
      end
      
      def options!(hsh)
        defaults = {
          :workers => 100,
          :debug   => false
        }

        @options = defaults.merge(hsh)
      end
      
      def options
        @options
      end
      
    end
    
    def initialize(db_config, options={})
      self.class.connect!(db_config)
      self.class.options!(options)
      @facts = []
    end

    def fact
      f = Fact.new
      yield f
      @facts << f
    end
    
    def extract!
      started = Time.now
      
      @facts.each {|f| f.run! }
      
      # puts @facts.inspect
      puts "took #{format_duration(Time.now - started)} "
    end
   
    private
    
    def format_duration(seconds)
      m, s = seconds.divmod(60)
      "#{m}minutes and #{'%.3f' % s}seconds" 
    end
    
        
  end
end