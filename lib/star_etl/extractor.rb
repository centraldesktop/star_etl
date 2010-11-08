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
            
    end
    
    def initialize(db_config)
      self.class.connect!(db_config)
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
      
      puts @facts.inspect
      puts "took #{Time.now - started} seconds"
    end
   
    private
        
  end
end