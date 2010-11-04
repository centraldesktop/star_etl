module StarEtl  
  class Extractor
    
    class << self
      def connect!(db_config)
        ActiveRecord::Base.establish_connection(db_config)
        @conn = ActiveRecord::Base.connection
      end
    
      def connection
        @conn
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
    
    def run!
      @facts.each do |fact|
        sql = %Q{SELECT * from #{fact.source} limit 1000}
        records = self.class.connection.execute(sql)
        
        records.each do |record|
          
          fact.dimensions.each do |dim_block|
            d = Dimension.new(record)
            dim_block.call(d)
            
            d.insert!
            
          end
          
        end
        
      end
      
      puts @facts.inspect
      
    end
    
  end
end