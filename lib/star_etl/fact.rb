module StarEtl  
  class Fact
    attr_accessor :source, :measure, :dest, :dimensions
  
    def initialize
      @dimensions = []
    end
  
    def dimension(&block)
      @dimensions << block
    end
  
    def method_missing(name, *args, &block)  
      puts "called #{name} with #{args.inspect}"
    end
    
    def run!
      sql = %Q{SELECT * from #{fact.source} limit 100}
      records = Extractor.connection.execute(sql)
    
      records.each do |record|
      
        fact.dimensions.each do |dim_block|
          d = Dimension.new(record)
          dim_block.call(d)
        
          d.insert!
        
        end
      
      end
    
    end
    
  end
end