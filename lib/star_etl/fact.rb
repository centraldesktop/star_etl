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
  end
end