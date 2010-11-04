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
      @threads = []
    end

    def fact
      f = Fact.new
      yield f
      @facts << f
    end
    
    def extract!
      started = Time.now
      
      @facts.each {|f| spawn(f) }
    
      #wait while they work
      until active_threads.empty?
        sleep(5)
      end
      
      puts @facts.inspect
      
      
      puts "took #{Time.now - started} seconds"
    end
   
    private
    
    def spawn(fact)
      t = Thread.new {fact.run!}

      @threads << t
    end
    
    def active_threads
      @threads.map(&:alive?).delete_if {|t| !t }
    end
    
    
  end
end