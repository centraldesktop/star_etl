module StarEtl  
  class Extractor
    
    class << self
      def connect!(db_config)
        @mutex = Mutex.new
        
        ActiveRecord::Base.establish_connection(db_config)
        @conn = ActiveRecord::Base.connection
      end
    
      def connection
        @mutex.synchronize { @conn }
      end
      
      def options!(hsh)
        defaults = {
          :workers    => 100,
          :batch_size => 200,
          :debug      => false
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

    def fact_source
      f = FactSource.new
      yield f
      @facts << f
    end
    
    def extract!
      started = Time.now
      @facts.each {|f| f.run! }
      puts "Finish in #{format_duration(Time.now - started)} "
    end
   
    private
    
    def format_duration(seconds)
      m, s = seconds.divmod(60)
      "#{m} minutes and #{'%.3f' % s} seconds" 
    end
    
        
  end
end