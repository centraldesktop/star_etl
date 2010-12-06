require 'star_etl/helper'
require 'star_etl/base'
require 'star_etl/fact'
require 'star_etl/dimension_factory'


module StarEtl
  
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
        :primary_key => "id",
        :debug       => false
      }

      @options = defaults.merge(hsh)
    end
    
    def options
      @options
    end
    
    def setup(db_config, opts={})
      connect!(db_config)
      options!(opts)
      @facts      = []
      @dimensions = []
    end
    
    def fact
      f = Fact.new
      yield f
      @facts << f
    end
    
    def aggregate
      f = Fact.new(true)
      yield f
      @facts << f
    end
    
    def dimension_factory
      d = DimensionFactory.new
      yield d
      @dimensions << d
    end

    def start!
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
